#include "session/client_session.hpp"
#include "common/log.hpp"
#include "config/config.hpp"
#include "pool/backend_connection_pool.hpp"
#include "pool/connection_wait_queue.hpp"
#include "protocol/error_response.hpp"
#include "protocol/message.hpp"
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/event.h>
#include <event2/util.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netdb.h>
#include <fcntl.h>
#include <cerrno>
#include <cstring>
#include <chrono>
#include <optional>

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif

namespace pgpooler {
namespace session {

namespace {

static int g_next_session_id = 1;
const char DISCARD_ALL[] = "DISCARD ALL";

}  // namespace

ClientSession::ClientSession(struct event_base* base, evutil_socket_t client_fd,
                             const std::string& client_addr,
                             pgpooler::config::BackendResolver resolver,
                             pgpooler::config::PoolManager* pool_manager,
                             pgpooler::pool::BackendConnectionPool* connection_pool,
                             pgpooler::pool::ConnectionWaitQueue* wait_queue,
                             const std::vector<std::uint8_t>* initial_data)
    : base_(base), client_fd_(client_fd), client_addr_(client_addr), resolver_(std::move(resolver)),
      pool_manager_(pool_manager), connection_pool_(connection_pool), wait_queue_(wait_queue),
      session_id_(g_next_session_id++) {
  client_input_ = evbuffer_new();
  if (!client_input_) { destroy(); return; }
  if (initial_data && !initial_data->empty())
    evbuffer_add(client_input_, initial_data->data(), initial_data->size());
  evutil_make_socket_nonblocking(client_fd_);
  int one = 1;
  setsockopt(client_fd_, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
  if (evbuffer_get_length(client_input_) > 0) {
    event_base_once(base_, -1, EV_TIMEOUT, [](evutil_socket_t, short, void* ctx) {
      static_cast<ClientSession*>(ctx)->on_client_read();
    }, this, nullptr);
  } else {
    ensure_client_read_event();
  }
}

ClientSession::~ClientSession() { destroy(); }

void ClientSession::destroy() {
  if (destroy_scheduled_) return;
  destroy_scheduled_ = true;
  if (wait_queue_) wait_queue_->remove(this);
  if (waiting_in_queue_) waiting_in_queue_ = false;
  if (pool_acquired_ && pool_manager_ && !backend_name_.empty()) {
    pool_manager_->release(backend_name_);
    pool_acquired_ = false;
  }
  if (bev_backend_) { bufferevent_free(bev_backend_); bev_backend_ = nullptr; }
  if (client_read_event_) { event_del(client_read_event_); event_free(client_read_event_); client_read_event_ = nullptr; }
  if (client_input_) { evbuffer_free(client_input_); client_input_ = nullptr; }
  if (client_fd_ >= 0) { evutil_closesocket(client_fd_); client_fd_ = -1; }
  delete this;
}

void ClientSession::on_client_read() {
  if (destroy_scheduled_) return;
  size_t input_len = client_input_ ? evbuffer_get_length(client_input_) : 0;

  if (state_ == State::ReadingFirst) {
    if (input_len < 8) return;
    size_t len = pgpooler::protocol::first_client_packet_length(client_input_);
    if (len == 0) return;
    pending_startup_.resize(len);
    evbuffer_remove(client_input_, pending_startup_.data(), len);
    auto user_opt = pgpooler::protocol::extract_startup_parameter(pending_startup_, "user");
    auto database_opt = pgpooler::protocol::extract_startup_parameter(pending_startup_, "database");
    user_ = user_opt.value_or("");
    database_ = database_opt.value_or("");
    connect_to_backend();
    return;
  }

  if (state_ == State::WaitingForBackend) {
    if (input_len < 5) return;
    connect_to_backend();
    return;
  }

  if (state_ == State::CollectingStartupResponse || state_ == State::Forwarding) {
    forward_client_to_backend();
    if (state_ == State::Forwarding && bev_backend_) {
      evutil_socket_t be_fd = bufferevent_getfd(bev_backend_);
      struct evbuffer* bin = bufferevent_get_input(bev_backend_);
      if (be_fd >= 0) evbuffer_read(bin, be_fd, 65536);
      while (state_ == State::Forwarding && bev_backend_) {
        bin = bufferevent_get_input(bev_backend_);
        if (evbuffer_get_length(bin) < 5) break;
        on_backend_read();
      }
    }
  }
}

void ClientSession::on_client_event(short what) {
  if (destroy_scheduled_) return;
  if ((what & (BEV_EVENT_EOF | BEV_EVENT_ERROR))) destroy();
}

void ClientSession::on_backend_read() {
  if (destroy_scheduled_ || !bev_backend_) return;
  struct evbuffer* input = bufferevent_get_input(bev_backend_);
  if (evbuffer_get_length(input) == 0) return;

  if (state_ == State::CollectingStartupResponse) {
    while (evbuffer_get_length(input) >= 5) {
      std::vector<std::uint8_t> msg;
      if (!pgpooler::protocol::try_extract_typed_message(input, msg)) break;
      unsigned char type = pgpooler::protocol::get_message_type(msg);
      cached_startup_response_.insert(cached_startup_response_.end(), msg.begin(), msg.end());
      client_out_buf_.insert(client_out_buf_.end(), msg.begin(), msg.end());
      flush_client_output();
      if (type == pgpooler::protocol::MSG_READY_FOR_QUERY) {
        auto state_opt = pgpooler::protocol::get_ready_for_query_state(msg);
        if (state_opt) {
          if (pool_mode_ == pgpooler::config::PoolMode::Session) {
            auto taken = connection_pool_->take(backend_name_, user_, database_, std::chrono::steady_clock::now(), server_idle_timeout_sec_, server_lifetime_sec_);
            if (taken) {
              close_auth_backend();
              bev_backend_ = taken->bev; taken->bev = nullptr;
              cached_startup_response_ = std::move(taken->cached_startup_response);
              backend_created_at_ = taken->created_at;
              pool_manager_->take_backend(backend_name_); pool_acquired_ = true;
              bufferevent_setcb(bev_backend_, [](struct bufferevent*, void* ctx) { static_cast<ClientSession*>(ctx)->on_backend_read(); }, nullptr, [](struct bufferevent*, short w, void* ctx) { static_cast<ClientSession*>(ctx)->on_backend_event(w); }, this);
              bufferevent_enable(bev_backend_, EV_READ);
              std::vector<std::uint8_t> discard = pgpooler::protocol::build_query_message(DISCARD_ALL);
              bufferevent_write(bev_backend_, discard.data(), discard.size());
              state_ = State::SendingDiscardAll;
            } else {
              state_ = State::Forwarding;
              start_forwarding();
              ensure_client_read_event();
            }
          } else {
            return_backend_to_pool();
          }
        }
        break;
      }
    }
    return;
  }

  if (state_ == State::SendingDiscardAll) {
    while (evbuffer_get_length(input) >= 5) {
      std::vector<std::uint8_t> msg;
      if (!pgpooler::protocol::try_extract_typed_message(input, msg)) break;
      if (pgpooler::protocol::get_message_type(msg) == pgpooler::protocol::MSG_READY_FOR_QUERY) {
        state_ = State::Forwarding;
        start_forwarding();
        ensure_client_read_event();
        forward_client_to_backend();
        break;
      }
    }
    return;
  }

  if (state_ == State::Forwarding) {
    while (evbuffer_get_length(input) >= 5) {
      std::vector<std::uint8_t> msg;
      if (!pgpooler::protocol::try_extract_typed_message(input, msg)) break;
      unsigned char type = pgpooler::protocol::get_message_type(msg);
      if (type == pgpooler::protocol::MSG_READY_FOR_QUERY) {
        auto state_opt = pgpooler::protocol::get_ready_for_query_state(msg);
        if (state_opt && (pool_mode_ == pgpooler::config::PoolMode::Statement || (pool_mode_ == pgpooler::config::PoolMode::Transaction && *state_opt == pgpooler::protocol::TXSTATE_IDLE))) {
          return_backend_to_pool();
          break;
        }
      }
      client_out_buf_.insert(client_out_buf_.end(), msg.begin(), msg.end());
    }
    flush_client_output();
  }
}

void ClientSession::on_backend_event(short what) {
  if (destroy_scheduled_) return;
  if (what & BEV_EVENT_CONNECTED) { on_backend_connected(); return; }
  if (what & (BEV_EVENT_EOF | BEV_EVENT_ERROR)) destroy();
}

void ClientSession::connect_to_backend() {
  if (user_.empty() && database_.empty()) return;
  auto resolved = resolver_(user_, database_);
  if (!resolved) { send_error_and_close("08000", "no backend for user/database"); return; }
  backend_name_ = resolved->name; backend_host_ = resolved->host; backend_port_ = resolved->port;
  pool_mode_ = resolved->pool_mode; server_idle_timeout_sec_ = resolved->server_idle_timeout_sec;
  server_lifetime_sec_ = resolved->server_lifetime_sec; query_wait_timeout_sec_ = resolved->query_wait_timeout_sec;

  if (state_ == State::WaitingForBackend) {
    auto now = std::chrono::steady_clock::now();
    auto taken = connection_pool_->take(backend_name_, user_, database_, now, server_idle_timeout_sec_, server_lifetime_sec_);
    if (taken) {
      pool_manager_->take_backend(backend_name_); pool_acquired_ = true;
      bev_backend_ = taken->bev; taken->bev = nullptr;
      cached_startup_response_ = std::move(taken->cached_startup_response);
      backend_created_at_ = taken->created_at;
      bufferevent_setcb(bev_backend_, [](struct bufferevent*, void* ctx) { static_cast<ClientSession*>(ctx)->on_backend_read(); }, nullptr, [](struct bufferevent*, short w, void* ctx) { static_cast<ClientSession*>(ctx)->on_backend_event(w); }, this);
      bufferevent_enable(bev_backend_, EV_READ);
      std::vector<std::uint8_t> discard = pgpooler::protocol::build_query_message(DISCARD_ALL);
      bufferevent_write(bev_backend_, discard.data(), discard.size());
      state_ = State::SendingDiscardAll;
      ensure_client_read_event();
      return;
    }
  }

  if (!pool_manager_->acquire(backend_name_)) {
    if (query_wait_timeout_sec_ > 0) { waiting_in_queue_ = true; wait_queue_->enqueue(this, backend_name_, user_, database_, query_wait_timeout_sec_); return; }
    send_error_and_close("53300", "sorry, too many clients already");
    return;
  }
  pool_acquired_ = true;
  backend_created_at_ = std::chrono::steady_clock::now();
  bev_backend_ = bufferevent_socket_new(base_, -1, BEV_OPT_CLOSE_ON_FREE);
  if (!bev_backend_) { pool_manager_->release(backend_name_); pool_acquired_ = false; send_error_and_close("08000", "failed to create backend connection"); return; }
  bufferevent_setcb(bev_backend_, [](struct bufferevent*, void* ctx) { static_cast<ClientSession*>(ctx)->on_backend_read(); }, nullptr, [](struct bufferevent*, short w, void* ctx) { static_cast<ClientSession*>(ctx)->on_backend_event(w); }, this);
  bufferevent_enable(bev_backend_, EV_READ);
  struct addrinfo hints = {}; hints.ai_family = AF_UNSPEC; hints.ai_socktype = SOCK_STREAM; hints.ai_flags = AI_NUMERICSERV;
  std::string port_str = std::to_string(backend_port_);
  struct addrinfo* res = nullptr;
  int gai = getaddrinfo(backend_host_.c_str(), port_str.c_str(), &hints, &res);
  if (gai != 0 || !res) {
    bufferevent_free(bev_backend_); bev_backend_ = nullptr; pool_manager_->release(backend_name_); pool_acquired_ = false;
    send_error_and_close("08000", std::string("invalid backend address: ") + (gai ? gai_strerror(gai) : "no address"));
    if (res) freeaddrinfo(res);
    return;
  }
  int conn_err = bufferevent_socket_connect(bev_backend_, res->ai_addr, static_cast<int>(res->ai_addrlen));
  freeaddrinfo(res);
  if (conn_err < 0) { bufferevent_free(bev_backend_); bev_backend_ = nullptr; pool_acquired_ = false; destroy(); return; }
  state_ = State::ConnectingToBackend;
}

void ClientSession::on_backend_connected() {
  bufferevent_write(bev_backend_, pending_startup_.data(), pending_startup_.size());
  state_ = State::CollectingStartupResponse;
  ensure_client_read_event();
}

void ClientSession::start_forwarding() {
  state_ = State::Forwarding;
}

void ClientSession::return_backend_to_pool() {
  if (!bev_backend_) return;
  flush_client_output();
  connection_pool_->put(backend_name_, user_, database_, bev_backend_, std::move(cached_startup_response_), backend_created_at_);
  bev_backend_ = nullptr;
  pool_manager_->put_backend(backend_name_); pool_acquired_ = false;
  state_ = State::WaitingForBackend;
  ensure_client_read_event();
  wait_queue_->on_connection_available(backend_name_, user_, database_);
}

void ClientSession::flush_client_output() {
  while (!client_out_buf_.empty() && client_fd_ >= 0) {
    ssize_t sent = send(client_fd_, client_out_buf_.data(), client_out_buf_.size(), MSG_NOSIGNAL);
    if (sent > 0) client_out_buf_.erase(client_out_buf_.begin(), client_out_buf_.begin() + static_cast<size_t>(sent));
    else if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return;
    else return;
  }
}

void ClientSession::forward_client_to_backend() {
  if (!bev_backend_ || !client_input_) return;
  while (evbuffer_get_length(client_input_) >= 5) {
    std::vector<std::uint8_t> msg;
    if (!pgpooler::protocol::try_extract_typed_message(client_input_, msg)) break;
    bufferevent_write(bev_backend_, msg.data(), msg.size());
  }
}

void ClientSession::ensure_client_read_event() {
  if (client_read_event_ || client_fd_ < 0) return;
  client_read_event_ = event_new(base_, client_fd_, EV_READ | EV_PERSIST, [](evutil_socket_t, short, void* ctx) {
    static_cast<ClientSession*>(ctx)->handle_client_read_event();
  }, this);
  if (client_read_event_) event_add(client_read_event_, nullptr);
}

void ClientSession::close_auth_backend() {
  if (!bev_backend_) return;
  pool_manager_->release(backend_name_); pool_acquired_ = false;
  bufferevent_free(bev_backend_); bev_backend_ = nullptr;
}

void ClientSession::send_error_and_close(const std::string& sqlstate, const std::string& message) {
  auto msg = pgpooler::protocol::build_error_response(sqlstate, message);
  if (client_fd_ >= 0) send(client_fd_, msg.data(), msg.size(), MSG_NOSIGNAL);
}

void ClientSession::handle_client_read_event() {
  if (destroy_scheduled_) return;
  ssize_t n = (client_fd_ >= 0 && client_input_) ? evbuffer_read(client_input_, client_fd_, -1) : 0;
  if (n <= 0) {
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return;
    destroy();
    return;
  }
  on_client_read();
}

void ClientSession::retry_connect_to_backend() { if (destroy_scheduled_) return; waiting_in_queue_ = false; connect_to_backend(); }
void ClientSession::on_wait_timeout() { if (destroy_scheduled_) return; waiting_in_queue_ = false; send_error_and_close("57P01", "query wait timeout"); destroy(); }

void ClientSession::schedule_flush_client() {
  if (client_out_buf_.empty()) return;
  event_base_once(base_, -1, EV_TIMEOUT, [](evutil_socket_t, short, void* ctx) {
    static_cast<ClientSession*>(ctx)->flush_client_output();
  }, this, nullptr);
}

}  // namespace session
}  // namespace pgpooler
