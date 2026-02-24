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
#include <netdb.h>
#include <sys/socket.h>
#include <cstring>
#include <chrono>
#include <deque>
#include <optional>

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif

namespace pgpooler {
namespace session {

namespace {

constexpr std::uint32_t SSL_REQUEST_CODE = 80877103;

const char* msg_type_name(unsigned char c) {
  switch (c) {
    case 'R': return "Auth/RowDesc";
    case 'E': return "Error";
    case 'N': return "Notice";
    case 'A': return "Notify";
    case 'S': return "ParamStatus";
    case 'K': return "BackendKey";
    case 'Z': return "ReadyForQuery";
    case 'T': return "RowDesc";
    case 'D': return "DataRow";
    case 'I': return "EmptyQuery";
    case 'C': return "CommandComplete";
    case 'n': return "NoData";
    case 't': return "ParseComplete";
    case '1': return "BindComplete";
    case '2': return "CloseComplete";
    default: return "?";
  }
}

const char* state_name(pgpooler::session::ClientSession::State s) {
  using S = pgpooler::session::ClientSession::State;
  switch (s) {
    case S::ReadingFirst: return "ReadingFirst";
    case S::ConnectingToBackend: return "ConnectingToBackend";
    case S::WaitingSSLResponse: return "WaitingSSLResponse";
    case S::CollectingStartupResponse: return "CollectingStartupResponse";
    case S::SendingDiscardAll: return "SendingDiscardAll";
    case S::WaitingForBackend: return "WaitingForBackend";
    case S::Forwarding: return "Forwarding";
    default: return "?";
  }
}

void static_client_read_cb(evutil_socket_t fd, short what, void* ctx) {
  (void)what;
  auto* self = static_cast<ClientSession*>(ctx);
  self->handle_client_read_event();
}

void static_backend_event_cb(struct bufferevent* bev, short what, void* ctx) {
  auto* self = static_cast<ClientSession*>(ctx);
  self->on_backend_event(what);
}

void static_backend_read_cb(struct bufferevent* bev, void* ctx) {
  (void)bev;
  auto* self = static_cast<ClientSession*>(ctx);
  self->on_backend_read();
}

void flush_once_cb(evutil_socket_t, short, void* ctx) {
  static_cast<ClientSession*>(ctx)->flush_client_output();
}

void static_client_write_cb(evutil_socket_t, short, void* ctx) {
  static_cast<ClientSession*>(ctx)->on_client_writable();
}

/** Free bufferevent in next event loop iteration (must not free inside its own callback). */
struct DeferredFreeBev {
  struct bufferevent* bev = nullptr;
};
void deferred_free_bev_cb(evutil_socket_t, short, void* ctx) {
  auto* h = static_cast<DeferredFreeBev*>(ctx);
  if (h->bev) bufferevent_free(h->bev);
  delete h;
}

std::string worker_prefix(int worker_id) {
  if (worker_id < 0) return "";
  return "[worker " + std::to_string(worker_id) + "] ";
}

}  // namespace

void ClientSession::static_deferred_destroy_cb(evutil_socket_t, short, void* ctx) {
  static_cast<ClientSession*>(ctx)->destroy();
}

ClientSession::ClientSession(struct event_base* base, evutil_socket_t client_fd,
                             const std::string& client_addr,
                             pgpooler::config::BackendResolver resolver,
                             pgpooler::config::PoolManager* pool_manager,
                             pgpooler::pool::BackendConnectionPool* connection_pool,
                             pgpooler::pool::ConnectionWaitQueue* wait_queue,
                             const std::vector<std::uint8_t>* initial_data,
                             int worker_id)
    : base_(base),
      client_addr_(client_addr),
      resolver_(std::move(resolver)),
      pool_manager_(pool_manager),
      wait_queue_(wait_queue),
      connection_pool_(connection_pool),
      client_fd_(client_fd),
      worker_id_(worker_id) {
  client_input_ = evbuffer_new();
  if (!client_input_) {
    pgpooler::log::error("client_session: evbuffer_new failed");
    destroy();
    return;
  }
  if (initial_data && !initial_data->empty()) {
    evbuffer_add(client_input_, initial_data->data(), initial_data->size());
  }
  session_id_ = static_cast<int>(client_fd_);
  client_read_event_ = event_new(base_, client_fd_, EV_READ | EV_PERSIST, static_client_read_cb, this);
  if (!client_read_event_) {
    evbuffer_free(client_input_);
    client_input_ = nullptr;
    destroy();
    return;
  }
  event_add(client_read_event_, nullptr);
  if (initial_data && !initial_data->empty()) {
    on_client_read();
  }
}

ClientSession::~ClientSession() {
  destroy();
}

void ClientSession::handle_client_read_event() {
  int n = evbuffer_read(client_input_, client_fd_, -1);
  if (n <= 0) {
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return;
    pgpooler::log::debug("client disconnected (EOF or error) fd=" + std::to_string(client_fd_), session_id_);
    destroy();
    return;
  }
  pgpooler::log::debug(worker_prefix(worker_id_) + "session: client read n=" + std::to_string(n) + " client_input_len=" + std::to_string(evbuffer_get_length(client_input_)), session_id_);
  on_client_read();
}

void ClientSession::on_client_read() {
  if (deferred_destroy_pending_ || destroy_scheduled_ || !client_input_) return;
  size_t client_in_len = client_input_ ? evbuffer_get_length(client_input_) : 0;
  pgpooler::log::debug(worker_prefix(worker_id_) + "session: on_client_read state=" + state_name(state_) + " client_input_len=" + std::to_string(client_in_len) + " pending_return=" + (pending_return_to_pool_ ? "1" : "0") + " bev_backend=" + (bev_backend_ ? "1" : "0"), session_id_);

  if (state_ == State::Forwarding) {
    if (pending_return_to_pool_) {
      pgpooler::log::debug(worker_prefix(worker_id_) + "session: Forwarding skip (pending_return_to_pool) client_out_buf=" + std::to_string(client_out_buf_.size()), session_id_);
      return;  // wait for previous response to be sent before sending next request
    }
    forward_client_to_backend();
    if (bev_backend_) {
      struct evbuffer* bin = bufferevent_get_input(bev_backend_);
      evutil_socket_t backend_fd = bufferevent_getfd(bev_backend_);
      for (;;) {
        int r = evbuffer_read(bin, backend_fd, 65536);
        size_t bin_len = evbuffer_get_length(bin);
        pgpooler::log::debug(worker_prefix(worker_id_) + "session: pump backend read r=" + std::to_string(r) + " bin_len=" + std::to_string(bin_len), session_id_);
        if (r <= 0) break;
        while (evbuffer_get_length(bin) >= 5) {
          on_backend_read();
          if (deferred_destroy_pending_ || destroy_scheduled_ || !bev_backend_) return;
          if (pending_return_to_pool_) {
            pgpooler::log::debug(worker_prefix(worker_id_) + "session: pump stop (pending_return) client_out_buf=" + std::to_string(client_out_buf_.size()), session_id_);
            return;
          }
        }
      }
    }
    return;
  }

  if (state_ == State::CollectingStartupResponse || state_ == State::SendingDiscardAll) {
    forward_client_to_backend();
    return;
  }

  if (state_ == State::WaitingForBackend) {
    // Client may have sent Terminate ('X' + length 4) after we returned the backend — don't take a new connection.
    if (client_in_len >= 5) {
      unsigned char* p = evbuffer_pullup(client_input_, 5);
      if (p && p[0] == 'X') {
        std::uint32_t len = (static_cast<std::uint32_t>(p[1]) << 24) | (static_cast<std::uint32_t>(p[2]) << 16) |
                            (static_cast<std::uint32_t>(p[3]) << 8) | static_cast<std::uint32_t>(p[4]);
        if (len == 4) {
          evbuffer_drain(client_input_, 5);
          pgpooler::log::debug(worker_prefix(worker_id_) + "session: client sent Terminate while WaitingForBackend, closing", session_id_);
          destroy();
          return;
        }
      }
    }
    auto idle = connection_pool_->take(backend_name_, user_, database_,
                                       std::chrono::steady_clock::now(),
                                       server_idle_timeout_sec_, server_lifetime_sec_);
    if (idle) {
      if (!pool_manager_->take_backend(backend_name_)) {
        connection_pool_->put(backend_name_, user_, database_, idle->bev,
                              std::move(idle->cached_startup_response), idle->created_at);
        return;
      }
      pgpooler::log::info(worker_prefix(worker_id_) + "session: took from pool backend=" + backend_name_ + " user=" + user_ + " database=" + database_ + " (next query) -> state=SendingDiscardAll sending DISCARD ALL", session_id_);
      pool_acquired_ = true;
      bev_backend_ = idle->bev;
      cached_startup_response_ = std::move(idle->cached_startup_response);
      backend_created_at_ = idle->created_at;
      bufferevent_setcb(bev_backend_, static_backend_read_cb, nullptr, static_backend_event_cb, this);
      bufferevent_enable(bev_backend_, EV_READ);
      std::vector<std::uint8_t> discard = protocol::build_query_message("DISCARD ALL");
      bufferevent_write(bev_backend_, discard.data(), discard.size());
      state_ = State::SendingDiscardAll;
      return;
    }
    if (!pool_manager_->acquire(backend_name_)) {
      pgpooler::log::info(worker_prefix(worker_id_) + "session: pool full, waiting in queue backend=" + backend_name_ + " user=" + user_ + " database=" + database_, session_id_);
      wait_queue_->enqueue(this, backend_name_, user_, database_, query_wait_timeout_sec_ ? query_wait_timeout_sec_ : 60);
      waiting_in_queue_ = true;
      return;
    }
    pgpooler::log::debug(worker_prefix(worker_id_) + "session: no idle in pool, connecting to backend -> state=ConnectingToBackend", session_id_);
    connect_to_backend();
    return;
  }

  if (state_ != State::ReadingFirst) return;

  for (;;) {
    size_t avail = evbuffer_get_length(client_input_);
    if (avail >= 8) {
      unsigned char* p = evbuffer_pullup(client_input_, 8);
      if (!p) break;
      std::uint32_t len = (static_cast<std::uint32_t>(p[0]) << 24) | (static_cast<std::uint32_t>(p[1]) << 16) |
                          (static_cast<std::uint32_t>(p[2]) << 8) | static_cast<std::uint32_t>(p[3]);
      std::uint32_t code = (static_cast<std::uint32_t>(p[4]) << 24) | (static_cast<std::uint32_t>(p[5]) << 16) |
                           (static_cast<std::uint32_t>(p[6]) << 8) | static_cast<std::uint32_t>(p[7]);
      if (len == 8 && code == SSL_REQUEST_CODE) {
        evbuffer_drain(client_input_, 8);
        const char no_ssl = 'N';
        ssize_t sent = send(client_fd_, &no_ssl, 1, MSG_NOSIGNAL);
        if (sent != 1) {
          pgpooler::log::warn("client_session: failed to send SSL N");
          destroy();
          return;
        }
        continue;
      }
    }
    break;
  }

  if (!protocol::try_extract_length_prefixed_message(client_input_, msg_buf_)) return;

  std::vector<std::uint8_t> startup_msg = msg_buf_;
  if (startup_msg.size() >= 8) {
    std::uint32_t len = (static_cast<std::uint32_t>(startup_msg[0]) << 24) |
                        (static_cast<std::uint32_t>(startup_msg[1]) << 16) |
                        (static_cast<std::uint32_t>(startup_msg[2]) << 8) |
                        static_cast<std::uint32_t>(startup_msg[3]);
    std::uint32_t code = (static_cast<std::uint32_t>(startup_msg[4]) << 24) |
                         (static_cast<std::uint32_t>(startup_msg[5]) << 16) |
                         (static_cast<std::uint32_t>(startup_msg[6]) << 8) |
                         static_cast<std::uint32_t>(startup_msg[7]);
    if (len == 8 && code == SSL_REQUEST_CODE && startup_msg.size() > 12) {
      startup_msg.erase(startup_msg.begin(), startup_msg.begin() + 12);
    }
  }
  auto user_opt = protocol::extract_startup_parameter(startup_msg, "user");
  auto db_opt = protocol::extract_startup_parameter(startup_msg, "database");
  user_ = user_opt ? *user_opt : "";
  database_ = db_opt ? *db_opt : "";

  auto resolved = resolver_(user_, database_);
  if (!resolved) {
    send_error_and_close("3D000", "no route for user/database");
    return;
  }
  backend_name_ = resolved->name;
  backend_host_ = resolved->host;
  backend_port_ = resolved->port;
  pool_mode_ = resolved->pool_mode;
  server_idle_timeout_sec_ = resolved->server_idle_timeout_sec;
  server_lifetime_sec_ = resolved->server_lifetime_sec;
  query_wait_timeout_sec_ = resolved->query_wait_timeout_sec;
  pending_startup_ = msg_buf_;
  client_startup_cache_ = startup_msg;

  if (pool_mode_ == pgpooler::config::PoolMode::Session) {
    auto idle = connection_pool_->take(backend_name_, user_, database_,
                                      std::chrono::steady_clock::now(),
                                      server_idle_timeout_sec_, server_lifetime_sec_);
    if (idle) {
      if (!pool_manager_->take_backend(backend_name_)) {
        connection_pool_->put(backend_name_, user_, database_, idle->bev,
                              std::move(idle->cached_startup_response), idle->created_at);
        send_error_and_close("53300", "pool error");
        return;
      }
      pgpooler::log::info(worker_prefix(worker_id_) + "session: took from pool backend=" + backend_name_ + " user=" + user_ + " database=" + database_ + " (session mode)", session_id_);
      pool_acquired_ = true;
      bev_backend_ = idle->bev;
      cached_startup_response_ = std::move(idle->cached_startup_response);
      backend_created_at_ = idle->created_at;
      bufferevent_setcb(bev_backend_, static_backend_read_cb, nullptr, static_backend_event_cb, this);
      bufferevent_enable(bev_backend_, EV_READ);
      client_out_buf_.insert(client_out_buf_.end(), cached_startup_response_.begin(), cached_startup_response_.end());
      flush_client_output();
      state_ = State::Forwarding;
      return;
    }
  }

  if (!pool_manager_->acquire(backend_name_)) {
    pgpooler::log::info(worker_prefix(worker_id_) + "session: pool full, waiting in queue backend=" + backend_name_ + " user=" + user_ + " database=" + database_, session_id_);
    wait_queue_->enqueue(this, backend_name_, user_, database_, query_wait_timeout_sec_ ? query_wait_timeout_sec_ : 60);
    waiting_in_queue_ = true;
    return;
  }
  pool_acquired_ = true;
  pgpooler::log::info(worker_prefix(worker_id_) + "session: new backend connection backend=" + backend_name_ + " user=" + user_ + " database=" + database_, session_id_);
  connect_to_backend();
}

void ClientSession::connect_to_backend() {
  state_ = State::ConnectingToBackend;
  char port_buf[16];
  snprintf(port_buf, sizeof(port_buf), "%u", backend_port_);
  struct addrinfo hints = {};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  struct addrinfo* res = nullptr;
  int err = getaddrinfo(backend_host_.c_str(), port_buf, &hints, &res);
  if (err != 0 || !res) {
    pgpooler::log::error("client_session: getaddrinfo failed: " + std::string(gai_strerror(err)));
    send_error_and_close("08006", "could not resolve backend host");
    return;
  }
  bev_backend_ = bufferevent_socket_new(base_, -1, BEV_OPT_CLOSE_ON_FREE);
  if (!bev_backend_) {
    freeaddrinfo(res);
    send_error_and_close("08006", "bufferevent_socket_new failed");
    return;
  }
  bufferevent_setcb(bev_backend_, static_backend_read_cb, nullptr, static_backend_event_cb, this);
  bufferevent_enable(bev_backend_, EV_READ);
  if (bufferevent_socket_connect(bev_backend_, res->ai_addr, static_cast<int>(res->ai_addrlen)) != 0) {
    pgpooler::log::error("client_session: bufferevent_socket_connect failed");
    bufferevent_free(bev_backend_);
    bev_backend_ = nullptr;
    freeaddrinfo(res);
    send_error_and_close("08006", "connect failed");
    return;
  }
  freeaddrinfo(res);
  backend_created_at_ = std::chrono::steady_clock::now();
}

void ClientSession::on_backend_connected() {
  if (state_ != State::ConnectingToBackend || !bev_backend_) return;
  state_ = State::CollectingStartupResponse;
  pgpooler::log::debug(worker_prefix(worker_id_) + "session: backend connected, sending startup backend=" + backend_name_, session_id_);
  bufferevent_write(bev_backend_, pending_startup_.data(), pending_startup_.size());
}

void ClientSession::on_backend_read() {
  if (deferred_destroy_pending_ || destroy_scheduled_ || !bev_backend_) return;
  struct evbuffer* bin = bufferevent_get_input(bev_backend_);
  if (state_ == State::CollectingStartupResponse) {
    while (evbuffer_get_length(bin) >= 5) {
      if (!protocol::try_extract_typed_message(bin, msg_buf_)) break;
      unsigned char mt = protocol::get_message_type(msg_buf_);
      size_t out_before = client_out_buf_.size();
      cached_startup_response_.insert(cached_startup_response_.end(), msg_buf_.begin(), msg_buf_.end());
      client_out_buf_.insert(client_out_buf_.end(), msg_buf_.begin(), msg_buf_.end());
      pgpooler::log::debug(worker_prefix(worker_id_) + "session: backend->client msg=" + std::string(msg_type_name(mt)) + " len=" + std::to_string(msg_buf_.size()) + " out_buf " + std::to_string(out_before) + "->" + std::to_string(client_out_buf_.size()), session_id_);
      flush_client_output();
      if (deferred_destroy_pending_) return;
      if (mt == protocol::MSG_READY_FOR_QUERY) {
        if (pool_mode_ == pgpooler::config::PoolMode::Session) {
          auto idle = connection_pool_->take(backend_name_, user_, database_,
                                             std::chrono::steady_clock::now(),
                                             server_idle_timeout_sec_, server_lifetime_sec_);
          if (idle) {
            pool_manager_->release(backend_name_);
            pool_acquired_ = false;
            {  // Defer free: must not free bev inside its read callback (causes heap corruption)
              struct bufferevent* to_free = bev_backend_;
              bev_backend_ = nullptr;
              DeferredFreeBev* h = new DeferredFreeBev{to_free};
              event_base_once(base_, -1, 0, deferred_free_bev_cb, h, nullptr);
            }
            if (!pool_manager_->take_backend(backend_name_)) {
              connection_pool_->put(backend_name_, user_, database_, idle->bev,
                                    std::move(idle->cached_startup_response), idle->created_at);
              return;
            }
            pgpooler::log::info(worker_prefix(worker_id_) + "session: auth done, took from pool backend=" + backend_name_ + " (session mode), sending DISCARD ALL", session_id_);
            pool_acquired_ = true;
            bev_backend_ = idle->bev;
            cached_startup_response_ = std::move(idle->cached_startup_response);
            backend_created_at_ = idle->created_at;
            bufferevent_setcb(bev_backend_, static_backend_read_cb, nullptr, static_backend_event_cb, this);
            bufferevent_enable(bev_backend_, EV_READ);
            std::vector<std::uint8_t> discard = protocol::build_query_message("DISCARD ALL");
            bufferevent_write(bev_backend_, discard.data(), discard.size());
            state_ = State::SendingDiscardAll;
            return;
          }
          pgpooler::log::info(worker_prefix(worker_id_) + "session: auth done, using auth connection backend=" + backend_name_ + " (session mode, pool empty)", session_id_);
        }
        if (pool_mode_ != pgpooler::config::PoolMode::Session) {
          pgpooler::log::info(worker_prefix(worker_id_) + "session: auth done, put auth connection to pool backend=" + backend_name_ + " user=" + user_ + " database=" + database_ + " mode=" + (pool_mode_ == pgpooler::config::PoolMode::Transaction ? "transaction" : "statement"), session_id_);
          bufferevent_setcb(bev_backend_, nullptr, nullptr, nullptr, nullptr);
          connection_pool_->put(backend_name_, user_, database_, bev_backend_,
                                std::move(cached_startup_response_), backend_created_at_);
          pool_manager_->put_backend(backend_name_);
          pool_acquired_ = false;
          bev_backend_ = nullptr;
          state_ = State::WaitingForBackend;
          wait_queue_->on_connection_available(backend_name_, user_, database_);
          return;
        }
        state_ = State::Forwarding;
        return;
      }
    }
    return;
  }
  if (state_ == State::SendingDiscardAll) {
    while (evbuffer_get_length(bin) >= 5) {
      if (!protocol::try_extract_typed_message(bin, msg_buf_)) break;
      unsigned char mt = protocol::get_message_type(msg_buf_);
      pgpooler::log::debug(worker_prefix(worker_id_) + "session: [DISCARD] consumed msg=" + std::string(msg_type_name(mt)) + " len=" + std::to_string(msg_buf_.size()) + " (not forwarded to client)", session_id_);
      if (deferred_destroy_pending_) return;
      if (mt == protocol::MSG_READY_FOR_QUERY) {
        pgpooler::log::debug(worker_prefix(worker_id_) + "session: DISCARD ALL done, forwarding backend=" + backend_name_, session_id_);
        state_ = State::Forwarding;
        forward_client_to_backend();
        return;
      }
    }
    return;
  }
  if (state_ == State::Forwarding) {
    while (evbuffer_get_length(bin) >= 5) {
      if (!protocol::try_extract_typed_message(bin, msg_buf_)) break;
      unsigned char mt = protocol::get_message_type(msg_buf_);
      size_t out_before = client_out_buf_.size();
      client_out_buf_.insert(client_out_buf_.end(), msg_buf_.begin(), msg_buf_.end());
      pgpooler::log::debug(worker_prefix(worker_id_) + "session: backend->client msg=" + std::string(msg_type_name(mt)) + " len=" + std::to_string(msg_buf_.size()) + " out_buf " + std::to_string(out_before) + "->" + std::to_string(client_out_buf_.size()), session_id_);
      flush_client_output();
      if (deferred_destroy_pending_) return;
      if (mt == protocol::MSG_READY_FOR_QUERY) {
        auto state_byte = protocol::get_ready_for_query_state(msg_buf_);
        bool return_now = (pool_mode_ == pgpooler::config::PoolMode::Statement) ||
                          (pool_mode_ == pgpooler::config::PoolMode::Transaction && state_byte == protocol::TXSTATE_IDLE);
        if (return_now) {
          if (!pending_return_to_pool_) {
            pgpooler::log::info(worker_prefix(worker_id_) + "session: returning connection to pool backend=" + backend_name_ + " user=" + user_ + " database=" + database_, session_id_);
            return_backend_to_pool();
          }
          return;
        }
      }
    }
  }
}

void ClientSession::on_client_event(short what) {
  (void)what;
}

void ClientSession::on_backend_event(short what) {
  if (destroy_scheduled_) return;
  if (what & BEV_EVENT_CONNECTED) {
    on_backend_connected();
    return;
  }
  if (what & (BEV_EVENT_EOF | BEV_EVENT_ERROR)) {
    int bfd = bufferevent_getfd(bev_backend_);
    pgpooler::log::debug(worker_prefix(worker_id_) + "session: backend event EOF/ERROR fd=" + std::to_string(bfd) + " state=" + state_name(state_) + " what=" + std::to_string(what) + " client_out_buf=" + std::to_string(client_out_buf_.size()), session_id_);
    if (state_ == State::SendingDiscardAll) {
      pgpooler::log::info(worker_prefix(worker_id_) + "session: stale connection from pool, retrying backend=" + backend_name_, session_id_);
      {  // Defer free: must not free bev inside its event callback (causes heap corruption)
        struct bufferevent* to_free = bev_backend_;
        bev_backend_ = nullptr;
        if (pool_acquired_) {
          pool_manager_->release(backend_name_);
          pool_acquired_ = false;
        }
        DeferredFreeBev* h = new DeferredFreeBev{to_free};
        event_base_once(base_, -1, 0, deferred_free_bev_cb, h, nullptr);
      }
      state_ = State::WaitingForBackend;
      on_client_read();
      return;
    }
    backend_dead_ = true;
    pgpooler::log::debug(worker_prefix(worker_id_) + "session: backend dead, sending error to client", session_id_);
    send_error_and_close("08006", "backend connection lost");
  }
}

void ClientSession::forward_client_to_backend() {
  if (!bev_backend_) return;
  size_t forwarded = 0;
  while (evbuffer_get_length(client_input_) >= 5) {
    if (!protocol::try_extract_typed_message(client_input_, msg_buf_)) break;
    if (msg_buf_.size() >= 1) {
      char type = static_cast<char>(msg_buf_[0]);
      pgpooler::log::debug(worker_prefix(worker_id_) + "session: client->backend msg=" + std::string(1, type) + " len=" + std::to_string(msg_buf_.size()) + " backend=" + backend_name_, session_id_);
    }
    bufferevent_write(bev_backend_, msg_buf_.data(), msg_buf_.size());
    forwarded += msg_buf_.size();
  }
  if (forwarded) {
    pgpooler::log::debug(worker_prefix(worker_id_) + "session: forward_client_to_backend total_bytes=" + std::to_string(forwarded) + " client_input_remaining=" + std::to_string(evbuffer_get_length(client_input_)), session_id_);
  }
}

void ClientSession::flush_client_output() {
  while (!client_out_buf_.empty() && client_fd_ >= 0) {
    ssize_t n = send(client_fd_, client_out_buf_.data(), client_out_buf_.size(), MSG_NOSIGNAL);
    if (n <= 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        pgpooler::log::debug(worker_prefix(worker_id_) + "session: flush EAGAIN pending=" + std::to_string(client_out_buf_.size()) + " (will wait EV_WRITE)", session_id_);
        if (!client_write_event_) {
          client_write_event_ = event_new(base_, client_fd_, EV_WRITE, static_client_write_cb, this);
          if (client_write_event_) event_add(client_write_event_, nullptr);
        }
        return;
      }
      pgpooler::log::debug(worker_prefix(worker_id_) + "session: flush send failed n=" + std::to_string(n) + " errno=" + std::to_string(errno), session_id_);
      /* Must not destroy() here: flush can be called from on_backend_read() -> use-after-free and heap corruption. */
      deferred_destroy_pending_ = true;
      event_base_once(base_, -1, 0, static_deferred_destroy_cb, this, nullptr);
      return;
    }
    client_out_buf_.erase(client_out_buf_.begin(), client_out_buf_.begin() + static_cast<size_t>(n));
    pgpooler::log::debug(worker_prefix(worker_id_) + "session: flush sent " + std::to_string(n) + " remaining=" + std::to_string(client_out_buf_.size()), session_id_);
  }
}

void ClientSession::schedule_flush_client() {
  event_base_once(base_, -1, 0, flush_once_cb, this, nullptr);
}

void ClientSession::return_backend_to_pool() {
  if (!bev_backend_) return;
  pgpooler::log::debug(worker_prefix(worker_id_) + "session: return_backend_to_pool start client_out_buf=" + std::to_string(client_out_buf_.size()), session_id_);
  for (int i = 0; i < 500 && !client_out_buf_.empty(); ++i) {
    flush_client_output();
    if (destroy_scheduled_) return;
  }
  pending_return_to_pool_ = true;
  pgpooler::log::debug(worker_prefix(worker_id_) + "session: return_backend_to_pool pending_return=1 client_out_buf=" + std::to_string(client_out_buf_.size()) + " (wait EV_WRITE)", session_id_);
  if (!client_write_event_) {
    client_write_event_ = event_new(base_, client_fd_, EV_WRITE, static_client_write_cb, this);
    if (client_write_event_) event_add(client_write_event_, nullptr);
  }
}

void ClientSession::do_return_backend_to_pool() {
  if (!bev_backend_) return;
  pgpooler::log::debug(worker_prefix(worker_id_) + "session: do_return_backend_to_pool backend=" + backend_name_ + " user=" + user_ + " database=" + database_, session_id_);
  pending_return_to_pool_ = false;
  if (client_write_event_) {
    event_del(client_write_event_);
    event_free(client_write_event_);
    client_write_event_ = nullptr;
  }
  bufferevent_setcb(bev_backend_, nullptr, nullptr, nullptr, nullptr);
  std::vector<std::uint8_t> to_store = protocol::trim_startup_response_to_post_auth(cached_startup_response_);
  connection_pool_->put(backend_name_, user_, database_, bev_backend_,
                        std::move(to_store), backend_created_at_);
  pool_manager_->put_backend(backend_name_);
  pool_acquired_ = false;
  bev_backend_ = nullptr;
  state_ = State::WaitingForBackend;
  wait_queue_->on_connection_available(backend_name_, user_, database_);
}

void ClientSession::on_client_writable() {
  if (destroy_scheduled_) return;
  pgpooler::log::debug(worker_prefix(worker_id_) + "session: on_client_writable client_out_buf=" + std::to_string(client_out_buf_.size()) + " pending_return=" + (pending_return_to_pool_ ? "1" : "0"), session_id_);
  flush_client_output();
  if (client_out_buf_.empty()) {
    if (pending_return_to_pool_) {
      do_return_backend_to_pool();
      size_t buf_len = client_input_ ? evbuffer_get_length(client_input_) : 0;
      if (!destroy_scheduled_ && client_input_ && buf_len >= 5) {
        pgpooler::log::debug(worker_prefix(worker_id_) + "session: processing buffered client data (next query)", session_id_);
        on_client_read();
      }
    } else if (client_write_event_) {
      event_del(client_write_event_);
      event_free(client_write_event_);
      client_write_event_ = nullptr;
    }
  }
}

void ClientSession::close_auth_backend() {
  if (bev_backend_) {
    bufferevent_free(bev_backend_);
    bev_backend_ = nullptr;
  }
  if (pool_acquired_) {
    pool_manager_->release(backend_name_);
    pool_acquired_ = false;
  }
}

void ClientSession::send_error_and_close(const std::string& sqlstate, const std::string& message) {
  auto msg = protocol::build_error_response(sqlstate, message);
  client_out_buf_.insert(client_out_buf_.end(), msg.begin(), msg.end());
  flush_client_output();
  destroy();
}

void ClientSession::retry_connect_to_backend() {
  waiting_in_queue_ = false;
  if (state_ == State::WaitingForBackend && pool_manager_->acquire(backend_name_)) {
    pool_acquired_ = true;
    connect_to_backend();
  }
}

void ClientSession::on_wait_timeout() {
  waiting_in_queue_ = false;
  send_error_and_close("57100", "connection wait timeout");
}

void ClientSession::destroy() {
  if (destroy_scheduled_) return;
  destroy_scheduled_ = true;
  pgpooler::log::debug(worker_prefix(worker_id_) + "session: destroy started state=" + state_name(state_) + " backend_dead=" + (backend_dead_ ? "1" : "0") + " client_out_buf=" + std::to_string(client_out_buf_.size()) + " bev_backend=" + (bev_backend_ ? "1" : "0"), session_id_);
  if (waiting_in_queue_) {
    wait_queue_->remove(this);
    waiting_in_queue_ = false;
  }
  if (bev_backend_ && backend_dead_) {
    /* Defer free: destroy() can be called from on_backend_event (send_error_and_close). */
    struct bufferevent* to_free = bev_backend_;
    bev_backend_ = nullptr;
    if (pool_acquired_) {
      pool_manager_->release(backend_name_);
      pool_acquired_ = false;
    }
    DeferredFreeBev* h = new DeferredFreeBev{to_free};
    event_base_once(base_, -1, 0, deferred_free_bev_cb, h, nullptr);
  } else if (bev_backend_ && (state_ == State::Forwarding || state_ == State::SendingDiscardAll || pending_return_to_pool_)) {
    do_return_backend_to_pool();
  } else if (bev_backend_) {
    /* Defer free: destroy() may be reentered from backend callback in edge cases. */
    struct bufferevent* to_free = bev_backend_;
    bev_backend_ = nullptr;
    if (pool_acquired_) {
      pool_manager_->release(backend_name_);
      pool_acquired_ = false;
    }
    DeferredFreeBev* h = new DeferredFreeBev{to_free};
    event_base_once(base_, -1, 0, deferred_free_bev_cb, h, nullptr);
  } else if (pool_acquired_) {
    pool_manager_->release(backend_name_);
    pool_acquired_ = false;
  }
  pending_return_to_pool_ = false;
  if (client_write_event_) {
    event_del(client_write_event_);
    event_free(client_write_event_);
    client_write_event_ = nullptr;
  }
  if (client_read_event_) {
    event_del(client_read_event_);
    event_free(client_read_event_);
    client_read_event_ = nullptr;
  }
  if (client_input_) {
    evbuffer_free(client_input_);
    client_input_ = nullptr;
  }
  if (client_fd_ >= 0) {
    pgpooler::log::debug(worker_prefix(worker_id_) + "session: client closed fd=" + std::to_string(client_fd_), session_id_);
    evutil_closesocket(client_fd_);
    client_fd_ = -1;
  }
  delete this;
}

void ClientSession::start_forwarding() {
  state_ = State::Forwarding;
  forward_client_to_backend();
}

}  // namespace session
}  // namespace pgpooler
