#include "session/client_session.hpp"
#include "common/log.hpp"
#include "protocol/message.hpp"
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/event.h>
#include <event2/util.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <vector>
#ifdef MSG_NOSIGNAL
#define PGPOOLER_MSG_NOSIGNAL MSG_NOSIGNAL
#else
#define PGPOOLER_MSG_NOSIGNAL 0
#endif

namespace pgpooler {
namespace session {

namespace {

void client_read_event_cb(evutil_socket_t fd, short what, void* ctx) {
  static_cast<ClientSession*>(ctx)->handle_client_read_event();
}

void backend_read_cb(struct bufferevent* bev, void* ctx) {
  auto* self = static_cast<ClientSession*>(ctx);
  self->on_backend_read();
}

void backend_event_cb(struct bufferevent* bev, short what, void* ctx) {
  auto* self = static_cast<ClientSession*>(ctx);
  self->on_backend_event(what);
}

void deferred_delete_cb(evutil_socket_t, short, void* ctx) {
  delete static_cast<ClientSession*>(ctx);
}

/** Run in next event loop iteration to flush client output (avoids writing from backend read context). */
void deferred_flush_client_cb(evutil_socket_t, short, void* ctx) {
  auto* self = static_cast<ClientSession*>(ctx);
  self->flush_client_output();
}

int next_session_id() {
  static int id = 0;
  return ++id;
}

/** Force drain bufferevent output to socket (bufferevent_flush is no-op for socket bev). */
void force_write_to_socket(struct bufferevent* bev) {
  struct evbuffer* out = bufferevent_get_output(bev);
  size_t len = evbuffer_get_length(out);
  if (len == 0) return;
  evutil_socket_t fd = bufferevent_getfd(bev);
  if (fd < 0) return;
  int n = evbuffer_write(out, fd);
  (void)n;
}

}  // namespace

ClientSession::ClientSession(struct event_base* base, evutil_socket_t client_fd,
                             const std::string& backend_host, std::uint16_t backend_port)
    : base_(base), backend_host_(backend_host), backend_port_(backend_port),
      session_id_(next_session_id()), client_fd_(client_fd) {
  pgpooler::log::info("client connected fd=" + std::to_string(client_fd) +
                      " -> backend " + backend_host + ":" + std::to_string(backend_port),
                      session_id_);
  pgpooler::log::debug("session created state=ReadingFirst client_fd=" + std::to_string(client_fd),
                       session_id_);
  evutil_make_socket_nonblocking(client_fd_);
  int one = 1;
  setsockopt(client_fd_, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
  client_input_ = evbuffer_new();
  if (!client_input_) {
    pgpooler::log::error("evbuffer_new(client_input) failed", session_id_);
    evutil_closesocket(client_fd_);
    throw std::runtime_error("evbuffer_new failed");
  }
  client_read_event_ = event_new(base_, client_fd_, EV_READ | EV_PERSIST, client_read_event_cb, this);
  if (!client_read_event_) {
    evbuffer_free(client_input_);
    evutil_closesocket(client_fd_);
    throw std::runtime_error("event_new(client read) failed");
  }
  if (event_add(client_read_event_, nullptr) != 0) {
    event_free(client_read_event_);
    evbuffer_free(client_input_);
    evutil_closesocket(client_fd_);
    throw std::runtime_error("event_add failed");
  }
}

ClientSession::~ClientSession() {
  /* Object is only deleted from deferred_delete_cb; destroy() was already called. */
}

void ClientSession::handle_client_read_event() {
  if (!client_input_ || client_fd_ < 0) return;
  int n = evbuffer_read(client_input_, client_fd_, -1);
  if (n <= 0) {
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return;
    pgpooler::log::info("client eof or error (n=" + std::to_string(n) + ")", session_id_);
    destroy();
    return;
  }
  on_client_read();
}

void ClientSession::on_client_read() {
  if (state_ == State::ConnectingToBackend) {
    pgpooler::log::debug("on_client_read: skip, state=ConnectingToBackend", session_id_);
    return;
  }

  struct evbuffer* input = client_input_;
  size_t available = evbuffer_get_length(input);
  pgpooler::log::debug("on_client_read: state=" + std::to_string(static_cast<int>(state_)) +
                       " input_len=" + std::to_string(available), session_id_);
  if (state_ == State::Forwarding && available > 0 && available < 5) {
    pgpooler::log::info("client sent " + std::to_string(available) + " bytes (waiting for full message)", session_id_);
  }

  if (state_ == State::ReadingFirst) {
    if (!protocol::try_extract_length_prefixed_message(input, msg_buf_)) {
      pgpooler::log::debug("ReadingFirst: not enough data yet, available=" + std::to_string(available), session_id_);
      return;
    }
    pending_startup_ = msg_buf_;
    const bool is_ssl_request = (msg_buf_.size() == 8 &&
        (static_cast<uint32_t>(msg_buf_[4]) << 24 | static_cast<uint32_t>(msg_buf_[5]) << 16 |
         static_cast<uint32_t>(msg_buf_[6]) << 8 | static_cast<uint32_t>(msg_buf_[7])) == 80877103);
    if (is_ssl_request) {
      pgpooler::log::info("received SSL request from client, forwarding to backend", session_id_);
    } else {
      pgpooler::log::info("received startup from client len=" + std::to_string(msg_buf_.size()),
                          session_id_);
    }
    if (bev_backend_) {
      /* Already connected (e.g. after SSL response). Send real startup and start forwarding. */
      pgpooler::log::info("sending startup to backend len=" + std::to_string(pending_startup_.size()),
                          session_id_);
      bufferevent_write(bev_backend_, pending_startup_.data(), pending_startup_.size());
      pending_startup_.clear();
      start_forwarding();
    } else {
      connect_to_backend();
    }
    return;
  }

  while (protocol::try_extract_typed_message(input, msg_buf_)) {
    char type = msg_buf_.empty() ? '?' : static_cast<char>(msg_buf_[0]);
    pgpooler::log::info("client->backend type='" + std::string(1, type) +
                        "' len=" + std::to_string(msg_buf_.size()), session_id_);
    bufferevent_write(bev_backend_, msg_buf_.data(), msg_buf_.size());
    force_write_to_socket(bev_backend_);
    pgpooler::log::debug("client->backend queued, backend_out_len=" +
                         std::to_string(evbuffer_get_length(bufferevent_get_output(bev_backend_))),
                         session_id_);
  }
  if (state_ == State::Forwarding) {
    size_t remaining = evbuffer_get_length(input);
    if (remaining > 0)
      pgpooler::log::debug("Forwarding: waiting for more data (need 5+ bytes for typed message), have " +
                           std::to_string(remaining), session_id_);
  }
}

void ClientSession::on_client_event(short what) {
  pgpooler::log::debug("client event what=" + std::to_string(what) +
                       (what & BEV_EVENT_EOF ? " EOF" : "") +
                       (what & BEV_EVENT_ERROR ? " ERROR" : "") +
                       (what & BEV_EVENT_TIMEOUT ? " TIMEOUT" : ""), session_id_);
  if (what & (BEV_EVENT_EOF | BEV_EVENT_ERROR)) {
    pgpooler::log::info("client closed or error", session_id_);
    destroy();
  }
}

void ClientSession::on_backend_read() {
  struct evbuffer* input = bufferevent_get_input(bev_backend_);
  size_t input_len = evbuffer_get_length(input);
  pgpooler::log::debug("on_backend_read: state=" + std::to_string(static_cast<int>(state_)) +
                       " backend_input_len=" + std::to_string(input_len), session_id_);

  if (state_ == State::WaitingSSLResponse) {
    if (input_len < 1) return;
    unsigned char byte;
    evbuffer_remove(input, &byte, 1);
    pgpooler::log::info("SSL response from backend: " + std::string(1, static_cast<char>(byte)) +
                        (byte == 'S' ? " (SSL supported)" : byte == 'N' ? " (no SSL)" : ""),
                        session_id_);
    client_out_buf_.push_back(byte);
    pgpooler::log::debug("SSL response queued to client, client_out_buf_.size()=" +
                         std::to_string(client_out_buf_.size()), session_id_);
    flush_client_output();
    state_ = State::ReadingFirst;
    return;
  }

  while (protocol::try_extract_typed_message(input, msg_buf_)) {
    char type = msg_buf_.empty() ? '?' : static_cast<char>(msg_buf_[0]);
    size_t msg_len = msg_buf_.size();
    pgpooler::log::info("backend->client type='" + std::string(1, type) + "' len=" + std::to_string(msg_len), session_id_);
    client_out_buf_.insert(client_out_buf_.end(), msg_buf_.data(), msg_buf_.data() + msg_len);
    pgpooler::log::debug("backend->client queued, client_out_buf_.size()=" +
                         std::to_string(client_out_buf_.size()), session_id_);
    flush_client_output();
  }
}

void ClientSession::on_backend_event(short what) {
  pgpooler::log::debug("backend event what=" + std::to_string(what) +
                       (what & BEV_EVENT_CONNECTED ? " CONNECTED" : "") +
                       (what & BEV_EVENT_EOF ? " EOF" : "") +
                       (what & BEV_EVENT_ERROR ? " ERROR" : ""), session_id_);
  if (what & BEV_EVENT_CONNECTED) {
    if (what & BEV_EVENT_ERROR) {
      pgpooler::log::error("backend connect failed", session_id_);
      destroy();
      return;
    }
    evutil_socket_t be_fd = bufferevent_getfd(bev_backend_);
    pgpooler::log::info("backend connected", session_id_);
    pgpooler::log::debug("backend fd=" + std::to_string(be_fd), session_id_);
    on_backend_connected();
    return;
  }
  if (what & (BEV_EVENT_EOF | BEV_EVENT_ERROR)) {
    pgpooler::log::info("backend closed or error", session_id_);
    destroy();
  }
}

void ClientSession::connect_to_backend() {
  struct addrinfo hints;
  std::memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;

  pgpooler::log::info("connecting to backend " + backend_host_ + ":" + std::to_string(backend_port_),
                      session_id_);
  std::string port_str = std::to_string(backend_port_);
  struct addrinfo* result = nullptr;
  if (getaddrinfo(backend_host_.c_str(), port_str.c_str(), &hints, &result) != 0 || !result) {
    pgpooler::log::error("getaddrinfo failed for " + backend_host_ + ":" + port_str, session_id_);
    destroy();
    return;
  }

  struct sockaddr_storage addr_storage;
  socklen_t addrlen = 0;
  for (struct addrinfo* rp = result; rp != nullptr; rp = rp->ai_next) {
    if (rp->ai_addrlen <= sizeof(addr_storage)) {
      std::memcpy(&addr_storage, rp->ai_addr, rp->ai_addrlen);
      addrlen = static_cast<socklen_t>(rp->ai_addrlen);
      break;
    }
  }
  freeaddrinfo(result);
  if (addrlen == 0) {
    pgpooler::log::error("no suitable address for backend", session_id_);
    destroy();
    return;
  }

  pgpooler::log::debug("state -> ConnectingToBackend", session_id_);
  state_ = State::ConnectingToBackend;
  bev_backend_ = bufferevent_socket_new(base_, -1, BEV_OPT_CLOSE_ON_FREE);
  if (!bev_backend_) {
    pgpooler::log::error("bufferevent_socket_new(backend) failed", session_id_);
    destroy();
    return;
  }
  bufferevent_setcb(bev_backend_, backend_read_cb, nullptr, backend_event_cb, this);
  bufferevent_enable(bev_backend_, EV_READ | EV_WRITE);

  if (bufferevent_socket_connect(bev_backend_,
        reinterpret_cast<struct sockaddr*>(&addr_storage), addrlen) != 0) {
    pgpooler::log::error("bufferevent_socket_connect failed", session_id_);
    destroy();
  }
}

void ClientSession::on_backend_connected() {
  if (pending_startup_.empty()) {
    pgpooler::log::error("no pending startup (bug)", session_id_);
    destroy();
    return;
  }
  {
    evutil_socket_t fd = bufferevent_getfd(bev_backend_);
    if (fd >= 0) {
      int one = 1;
      setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    }
  }
  const bool is_ssl_request = (pending_startup_.size() == 8 &&
      (static_cast<uint32_t>(pending_startup_[4]) << 24 |
       static_cast<uint32_t>(pending_startup_[5]) << 16 |
       static_cast<uint32_t>(pending_startup_[6]) << 8 |
       static_cast<uint32_t>(pending_startup_[7])) == 80877103);
  pgpooler::log::info(is_ssl_request ? "sending SSL request to backend" :
                      "sending startup to backend len=" + std::to_string(pending_startup_.size()),
                      session_id_);
  bufferevent_write(bev_backend_, pending_startup_.data(), pending_startup_.size());
  pending_startup_.clear();
  if (is_ssl_request) {
    pgpooler::log::debug("state -> WaitingSSLResponse", session_id_);
    state_ = State::WaitingSSLResponse;
    pgpooler::log::info("waiting for SSL response (1 byte) from backend", session_id_);
  } else {
    start_forwarding();
  }
}

void ClientSession::start_forwarding() {
  pgpooler::log::debug("state -> Forwarding", session_id_);
  state_ = State::Forwarding;
  pgpooler::log::info("forwarding started (client <-> backend)", session_id_);
}

void ClientSession::schedule_flush_client() {
  struct timeval tv = {0, 0};
  event_base_once(base_, -1, EV_TIMEOUT, deferred_flush_client_cb, this, &tv);
}

void ClientSession::flush_client_output() {
  if (client_fd_ < 0 || client_out_buf_.empty()) return;
  evutil_socket_t fd = client_fd_;
  size_t len = client_out_buf_.size();
  pgpooler::log::debug("flush_client_output: fd=" + std::to_string(fd) + " len=" + std::to_string(len),
                       session_id_);
  ssize_t n = send(fd, client_out_buf_.data(), len, PGPOOLER_MSG_NOSIGNAL);
  if (n > 0) {
    size_t sent = static_cast<size_t>(n);
    client_out_buf_.erase(client_out_buf_.begin(), client_out_buf_.begin() + sent);
    pgpooler::log::debug("flush_client_output: sent " + std::to_string(n) + " bytes, pending " +
                         std::to_string(client_out_buf_.size()), session_id_);
  } else {
    int e = errno;
    pgpooler::log::debug("flush_client_output: send() failed errno=" + std::to_string(e) +
                         " pending=" + std::to_string(len), session_id_);
  }
}

void ClientSession::destroy() {
  if (destroy_scheduled_) return;
  destroy_scheduled_ = true;
  pgpooler::log::debug("destroy: freeing client event and backend", session_id_);
  pgpooler::log::info("session closed", session_id_);
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
    evutil_closesocket(client_fd_);
    client_fd_ = -1;
  }
  if (bev_backend_) {
    bufferevent_free(bev_backend_);
    bev_backend_ = nullptr;
  }
  event_base_once(base_, -1, EV_TIMEOUT, deferred_delete_cb, this, nullptr);
}

}  // namespace session
}  // namespace pgpooler
