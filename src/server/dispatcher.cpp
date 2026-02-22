#include "server/dispatcher.hpp"
#include "server/fd_send.hpp"
#include "common/log.hpp"
#include "config/config.hpp"
#include "pool/backend_connection_pool.hpp"
#include "pool/connection_wait_queue.hpp"
#include "protocol/message.hpp"
#include "session/client_session.hpp"
#include <event2/buffer.h>
#include <event2/event.h>
#include <event2/listener.h>
#include <event2/util.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <fcntl.h>
#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif
#include <cstring>
#include <functional>
#include <memory>
#include <unordered_map>

namespace pgpooler {
namespace server {

namespace {

struct DispatcherCtx {
  event_base* base = nullptr;
  std::vector<int> worker_fds;
  std::map<std::string, std::size_t> backend_to_worker;
  pgpooler::config::BackendResolver resolver;
};

struct DispatcherStub {
  evutil_socket_t client_fd = -1;
  evbuffer* input = nullptr;
  event* read_ev = nullptr;
  std::string client_addr;
  DispatcherCtx* dispatch_ctx = nullptr;
};

void stub_read_cb(evutil_socket_t fd, short what, void* ctx);

void stub_destroy(DispatcherStub* stub, event_base* base) {
  if (!stub) return;
  if (stub->read_ev) {
    event_del(stub->read_ev);
    event_free(stub->read_ev);
    stub->read_ev = nullptr;
  }
  if (stub->input) {
    evbuffer_free(stub->input);
    stub->input = nullptr;
  }
  if (stub->client_fd >= 0) {
    evutil_closesocket(stub->client_fd);
    stub->client_fd = -1;
  }
  delete stub;
}

void on_dispatch_accept(struct evconnlistener* /*listener*/, evutil_socket_t client_fd,
                        struct sockaddr* address, int /*socklen*/, void* ctx) {
  auto* dispatch_ctx = static_cast<DispatcherCtx*>(ctx);
  event_base* base = dispatch_ctx->base;
  evutil_make_socket_nonblocking(client_fd);
  int one = 1;
  setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
  char addr_buf[64] = {};
  if (address && address->sa_family == AF_INET) {
    auto* sa = reinterpret_cast<struct sockaddr_in*>(address);
    evutil_inet_ntop(AF_INET, &sa->sin_addr, addr_buf, sizeof(addr_buf));
  }
  pgpooler::log::info("dispatcher: new connection fd=" + std::to_string(client_fd) +
                      (addr_buf[0] ? std::string(" from ") + addr_buf : ""));

  auto* stub = new DispatcherStub();
  stub->client_fd = client_fd;
  stub->client_addr = addr_buf[0] ? addr_buf : "";
  stub->dispatch_ctx = dispatch_ctx;
  stub->input = evbuffer_new();
  if (!stub->input) {
    pgpooler::log::error("dispatcher: evbuffer_new failed for fd=" + std::to_string(client_fd));
    stub_destroy(stub, base);
    evutil_closesocket(client_fd);
    return;
  }
  stub->read_ev = event_new(base, client_fd, EV_READ | EV_PERSIST, stub_read_cb, stub);
  if (!stub->read_ev) {
    stub_destroy(stub, base);
    return;
  }
  event_assign(stub->read_ev, base, client_fd, EV_READ | EV_PERSIST, stub_read_cb, stub);
  event_add(stub->read_ev, nullptr);
}

void stub_read_cb(evutil_socket_t fd, short /*what*/, void* ctx) {
  auto* stub = static_cast<DispatcherStub*>(ctx);
  int n = evbuffer_read(stub->input, fd, -1);
  if (n <= 0) {
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return;
    pgpooler::log::debug("dispatcher: client fd=" + std::to_string(fd) + " eof/error n=" + std::to_string(n));
    stub_destroy(stub, event_get_base(stub->read_ev));
    return;
  }
  size_t avail = evbuffer_get_length(stub->input);
  if (avail >= 8) {
    unsigned char* p = evbuffer_pullup(stub->input, 8);
    if (p) {
      std::uint32_t len = (static_cast<std::uint32_t>(p[0]) << 24) | (static_cast<std::uint32_t>(p[1]) << 16) |
                          (static_cast<std::uint32_t>(p[2]) << 8) | static_cast<std::uint32_t>(p[3]);
      std::uint32_t code = (static_cast<std::uint32_t>(p[4]) << 24) | (static_cast<std::uint32_t>(p[5]) << 16) |
                           (static_cast<std::uint32_t>(p[6]) << 8) | static_cast<std::uint32_t>(p[7]);
      if (len == 8 && code == 80877103) {
        evbuffer_drain(stub->input, 8);
        const char no_ssl = 'N';
        ssize_t sent = send(fd, &no_ssl, 1, MSG_NOSIGNAL);
        if (sent != 1) {
          pgpooler::log::warn("dispatcher: failed to send SSL N to fd=" + std::to_string(fd));
          stub_destroy(stub, event_get_base(stub->read_ev));
          return;
        }
        pgpooler::log::debug("dispatcher: sent SSL N to fd=" + std::to_string(fd) + ", waiting for Startup");
        return;
      }
    }
  }
  size_t need = protocol::first_client_packet_length(stub->input);
  if (need == 0) return;
  pgpooler::log::debug("dispatcher: first packet complete fd=" + std::to_string(fd) + " len=" + std::to_string(need));

  std::vector<std::uint8_t> packet(need);
  size_t removed = evbuffer_remove(stub->input, packet.data(), need);
  if (removed != need) {
    stub_destroy(stub, event_get_base(stub->read_ev));
    return;
  }

  std::vector<std::uint8_t> startup_msg;
  if (packet.size() >= 8) {
    std::uint32_t len = (static_cast<std::uint32_t>(packet[0]) << 24) | (static_cast<std::uint32_t>(packet[1]) << 16) |
                        (static_cast<std::uint32_t>(packet[2]) << 8) | static_cast<std::uint32_t>(packet[3]);
    std::uint32_t code = (static_cast<std::uint32_t>(packet[4]) << 24) | (static_cast<std::uint32_t>(packet[5]) << 16) |
                         (static_cast<std::uint32_t>(packet[6]) << 8) | static_cast<std::uint32_t>(packet[7]);
    if (len == 8 && code == 80877103) {
      startup_msg.assign(packet.begin() + 8, packet.end());
    } else {
      startup_msg = packet;
    }
  } else {
    startup_msg = packet;
  }
  auto user = protocol::extract_startup_parameter(startup_msg, "user");
  auto database = protocol::extract_startup_parameter(startup_msg, "database");
  std::string user_s = user ? *user : "";
  std::string database_s = database ? *database : "";

  auto* base = event_get_base(stub->read_ev);
  DispatcherCtx* dispatch_ctx = stub->dispatch_ctx;
  if (!dispatch_ctx) {
    stub_destroy(stub, base);
    return;
  }

  auto resolved = dispatch_ctx->resolver(user_s, database_s);
  if (!resolved) {
    pgpooler::log::warn("dispatcher: no route for user=" + user_s + " database=" + database_s);
    stub_destroy(stub, base);
    return;
  }

  auto it = dispatch_ctx->backend_to_worker.find(resolved->name);
  std::size_t worker_id = (it != dispatch_ctx->backend_to_worker.end()) ? it->second : 0;
  if (worker_id >= dispatch_ctx->worker_fds.size()) {
    worker_id = 0;
  }
  int worker_fd = dispatch_ctx->worker_fds[worker_id];
  pgpooler::log::info("dispatcher: routing user=" + user_s + " database=" + database_s +
                      " -> backend=" + resolved->name + " worker=" + std::to_string(worker_id) +
                      " fd=" + std::to_string(stub->client_fd));

  evutil_socket_t client_fd = stub->client_fd;
  stub->client_fd = -1;
  event_del(stub->read_ev);
  event_free(stub->read_ev);
  stub->read_ev = nullptr;
  evbuffer_free(stub->input);
  stub->input = nullptr;
  delete stub;

  if (!send_fd_and_payload(worker_fd, static_cast<int>(client_fd), packet)) {
    pgpooler::log::error("dispatcher: send_fd_and_payload failed for worker=" + std::to_string(worker_id));
    evutil_closesocket(client_fd);
    return;
  }
  pgpooler::log::debug("dispatcher: handed off client fd=" + std::to_string(client_fd) + " to worker " + std::to_string(worker_id));
  evutil_closesocket(client_fd);
}

void dispatcher_listener_error_cb(struct evconnlistener* /*listener*/, void* /*ctx*/) {}

}  // namespace

void run_dispatcher(
    struct event_base* base,
    const std::string& listen_host,
    std::uint16_t listen_port,
    const std::vector<int>& worker_socket_fds,
    const std::map<std::string, std::size_t>& backend_to_worker,
    pgpooler::config::BackendResolver resolver) {
  DispatcherCtx ctx;
  ctx.base = base;
  ctx.worker_fds = worker_socket_fds;
  ctx.backend_to_worker = backend_to_worker;
  ctx.resolver = std::move(resolver);
  struct sockaddr_in sin;
  std::memset(&sin, 0, sizeof(sin));
  sin.sin_family = AF_INET;
  sin.sin_port = htons(listen_port);
  if (!listen_host.empty() && listen_host != "0.0.0.0") {
    evutil_inet_pton(AF_INET, listen_host.c_str(), &sin.sin_addr);
  } else {
    sin.sin_addr.s_addr = htonl(INADDR_ANY);
  }

  evconnlistener* listener = evconnlistener_new_bind(
      base, on_dispatch_accept, &ctx,
      LEV_OPT_CLOSE_ON_FREE | LEV_OPT_REUSEABLE,
      -1, reinterpret_cast<struct sockaddr*>(&sin), sizeof(sin));
  if (!listener) {
    pgpooler::log::error("dispatcher: failed to bind " + listen_host + ":" + std::to_string(listen_port));
    return;
  }
  evconnlistener_set_error_cb(listener, dispatcher_listener_error_cb);

  pgpooler::log::info("dispatcher listening on " + listen_host + ":" + std::to_string(listen_port) +
                      " (workers=" + std::to_string(worker_socket_fds.size()) + ")");
  event_base_dispatch(base);
  evconnlistener_free(listener);
}

namespace {

std::string resolve_path(const std::string& base_file, const std::string& path) {
  if (path.empty()) return path;
  if (path[0] == '/' || (path.size() >= 2 && path[1] == ':')) return path;
  std::string::size_type pos = base_file.find_last_of("/\\");
  if (pos == std::string::npos) return path;
  return base_file.substr(0, pos + 1) + path;
}

struct WorkerCtx {
  event_base* base = nullptr;
  int worker_socket_fd = -1;
  pgpooler::config::BackendResolver resolver;
  pgpooler::config::PoolManager* pool_manager = nullptr;
  pgpooler::pool::BackendConnectionPool* connection_pool = nullptr;
  pgpooler::pool::ConnectionWaitQueue* wait_queue = nullptr;
  WorkerRecvState recv_state;
};

void worker_socket_read_cb(evutil_socket_t fd, short /*what*/, void* ctx) {
  auto* wctx = static_cast<WorkerCtx*>(ctx);
  for (;;) {
    auto result = try_recv_fd_and_payload(static_cast<int>(fd), wctx->recv_state);
    if (!result) break;
    int client_fd = result->first;
    std::vector<std::uint8_t> payload = std::move(result->second);
    if (client_fd < 0) continue;
    evutil_make_socket_nonblocking(client_fd);
    pgpooler::log::info("worker: received client fd=" + std::to_string(client_fd) + " payload_len=" + std::to_string(payload.size()));
    try {
      (void)new pgpooler::session::ClientSession(
          wctx->base, client_fd, "dispatcher",
          wctx->resolver, wctx->pool_manager, wctx->connection_pool, wctx->wait_queue,
          &payload);
      pgpooler::log::debug("worker: session created for fd=" + std::to_string(client_fd));
    } catch (const std::exception& e) {
      pgpooler::log::error("worker: session create failed fd=" + std::to_string(client_fd) + ": " + e.what());
      evutil_closesocket(client_fd);
    } catch (...) {
      pgpooler::log::error("worker: session create failed fd=" + std::to_string(client_fd));
      evutil_closesocket(client_fd);
    }
  }
}

}  // namespace

void run_worker(
    std::size_t worker_id,
    int worker_socket_fd,
    const std::vector<std::string>& backend_names,
    const std::string& resolve_base_path,
    const std::string& backends_path,
    const std::string& routing_path) {
  pgpooler::config::AppConfig app_cfg;
  if (!pgpooler::config::load_app_config(resolve_base_path, app_cfg)) {
    std::cerr << "worker " << worker_id << ": failed to load app config from " << resolve_base_path << std::endl;
    return;
  }
  std::string logging_path = resolve_path(resolve_base_path, app_cfg.logging_config_path);
  pgpooler::config::LoggingConfig logging_cfg;
  if (pgpooler::config::load_logging_config(logging_path, logging_cfg)) {
    pgpooler::log::init(logging_cfg);
  }

  if (backend_names.empty()) {
    pgpooler::log::error("worker " + std::to_string(worker_id) + ": no backends");
    return;
  }
  std::string abs_backends = resolve_path(resolve_base_path, backends_path);
  std::string abs_routing = resolve_path(resolve_base_path, routing_path);
  pgpooler::log::debug("worker " + std::to_string(worker_id) + ": loading backends from " + abs_backends + " routing from " + abs_routing);

  pgpooler::config::BackendsConfig backends_cfg;
  if (!pgpooler::config::load_backends_config(abs_backends, backends_cfg)) {
    pgpooler::log::error("worker " + std::to_string(worker_id) + ": failed to load backends");
    return;
  }
  pgpooler::config::RoutingConfig routing_cfg;
  if (!pgpooler::config::load_routing_config(abs_routing, routing_cfg)) {
    pgpooler::log::error("worker " + std::to_string(worker_id) + ": failed to load routing");
    return;
  }

  std::vector<pgpooler::config::BackendEntry> filtered;
  for (const auto& be : backends_cfg.backends) {
    for (const auto& name : backend_names) {
      if (be.name == name) {
        filtered.push_back(be);
        break;
      }
    }
  }
  if (filtered.empty()) {
    pgpooler::log::error("worker " + std::to_string(worker_id) + ": no matching backends");
    return;
  }

  pgpooler::config::Router* router_ptr = nullptr;
  pgpooler::config::Router router(backends_cfg.backends, routing_cfg.defaults, routing_cfg.routing);
  if (!routing_cfg.routing.empty()) router_ptr = &router;
  pgpooler::config::BackendResolver resolver =
      pgpooler::config::make_resolver(backends_cfg.backends, routing_cfg, router_ptr);

  pgpooler::config::PoolManager pool_manager(filtered);
  pgpooler::pool::BackendConnectionPool connection_pool;

  event_base* base = event_base_new();
  if (!base) {
    pgpooler::log::error("worker " + std::to_string(worker_id) + ": event_base_new failed");
    return;
  }
  pgpooler::pool::ConnectionWaitQueue wait_queue(base);

  evutil_make_socket_nonblocking(worker_socket_fd);

  WorkerCtx wctx;
  wctx.base = base;
  wctx.worker_socket_fd = worker_socket_fd;
  wctx.resolver = std::move(resolver);
  wctx.pool_manager = &pool_manager;
  wctx.connection_pool = &connection_pool;
  wctx.wait_queue = &wait_queue;

  event* read_ev = event_new(base, worker_socket_fd, EV_READ | EV_PERSIST, worker_socket_read_cb, &wctx);
  if (!read_ev) {
    event_base_free(base);
    return;
  }
  event_add(read_ev, nullptr);

  pgpooler::log::info("worker " + std::to_string(worker_id) + " ready (backends: " + std::to_string(filtered.size()) + ")");
  event_base_dispatch(base);
  event_free(read_ev);
  event_base_free(base);
}

}  // namespace server
}  // namespace pgpooler
