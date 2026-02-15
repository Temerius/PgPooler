#pragma once

#include <cstdint>
#include <string>

struct event_base;
struct evconnlistener;

namespace pgpooler {
namespace server {

struct BackendConfig {
  std::string host;
  std::uint16_t port = 5432;
};

struct AcceptCtx {
  struct event_base* base = nullptr;
  BackendConfig backend;
};

class Listener {
 public:
  Listener(struct event_base* base, const char* listen_host, std::uint16_t listen_port,
           const BackendConfig& backend);
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
