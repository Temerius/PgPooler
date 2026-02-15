#include "server/listener.hpp"
#include "common/log.hpp"
#include "session/client_session.hpp"
#include <event2/listener.h>
#include <event2/util.h>
#include <netinet/in.h>
#include <cstring>
#include <string>

namespace pgpooler {
namespace server {

namespace {

void on_accept(struct evconnlistener* listener, evutil_socket_t client_fd,
               struct sockaddr* address, int /*socklen*/, void* ctx) {
  auto* accept_ctx = static_cast<AcceptCtx*>(ctx);
  evutil_make_socket_nonblocking(client_fd);
  char addr_buf[64];
  if (address && address->sa_family == AF_INET) {
    auto* sa = reinterpret_cast<struct sockaddr_in*>(address);
    evutil_inet_ntop(AF_INET, &sa->sin_addr, addr_buf, sizeof(addr_buf));
  } else {
    addr_buf[0] = '\0';
  }
  pgpooler::log::info(std::string("new connection fd=") + std::to_string(client_fd) +
                      (addr_buf[0] ? std::string(" from ") + addr_buf : ""));
  (void)new pgpooler::session::ClientSession(
      accept_ctx->base, client_fd,
      accept_ctx->backend.host, accept_ctx->backend.port);
}

void on_listener_error(struct evconnlistener* /*listener*/, void* /*ctx*/) {}

}  // namespace

Listener::Listener(struct event_base* base, const char* listen_host, std::uint16_t listen_port,
                   const BackendConfig& backend)
    : port_(listen_port), accept_ctx_{base, backend} {
  struct sockaddr_in sin;
  std::memset(&sin, 0, sizeof(sin));
  sin.sin_family = AF_INET;
  sin.sin_port = htons(listen_port);
  if (listen_host && listen_host[0] != '\0') {
    evutil_inet_pton(AF_INET, listen_host, &sin.sin_addr);
  } else {
    sin.sin_addr.s_addr = htonl(INADDR_ANY);
  }

  listener_ = evconnlistener_new_bind(
      base, on_accept, &accept_ctx_,
      LEV_OPT_CLOSE_ON_FREE | LEV_OPT_REUSEABLE,
      -1, reinterpret_cast<struct sockaddr*>(&sin), sizeof(sin));
  if (listener_) {
    evconnlistener_set_error_cb(listener_, on_listener_error);
  }
}

Listener::~Listener() {
  if (listener_) {
    evconnlistener_free(listener_);
    listener_ = nullptr;
  }
}

}  // namespace server
}  // namespace pgpooler
