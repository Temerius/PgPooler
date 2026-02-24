#pragma once

#include "config/config.hpp"
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>

struct event_base;
struct evconnlistener;

namespace pgpooler {
namespace pool {
class BackendConnectionPool;
class ConnectionWaitQueue;
}
namespace server {

using BackendResolver = pgpooler::config::BackendResolver;

struct AcceptCtx {
  struct event_base* base = nullptr;
  BackendResolver resolver;
  pgpooler::config::PoolManager* pool_manager = nullptr;
  pgpooler::pool::BackendConnectionPool* connection_pool = nullptr;
  pgpooler::pool::ConnectionWaitQueue* wait_queue = nullptr;
};

class Listener {
 public:
  Listener(struct event_base* base, const char* listen_host, std::uint16_t listen_port,
           BackendResolver resolver, pgpooler::config::PoolManager* pool_manager,
           pgpooler::pool::BackendConnectionPool* connection_pool,
           pgpooler::pool::ConnectionWaitQueue* wait_queue);
  ~Listener();

  Listener(const Listener&) = delete;
  Listener& operator=(const Listener&) = delete;

  bool ok() const { return listener_ != nullptr; }
  std::uint16_t port() const { return port_; }

 private:
  struct evconnlistener* listener_ = nullptr;
  std::uint16_t port_ = 0;
  AcceptCtx accept_ctx_;
};

}  // namespace server
}  // namespace pgpooler
