#pragma once

#include "config/config.hpp"
#include <event2/util.h>
#include <cstdint>
#include <string>
#include <vector>

struct event_base;
struct bufferevent;
struct event;
struct evbuffer;

namespace pgpooler {
namespace session {

/** Holds client connection and proxies to a single PostgreSQL backend.
 * State: read first (length-prefixed) message from client → resolve backend →
 * connect to backend → send first message → then forward messages both ways. */
class ClientSession {
 public:
  /** Uses resolver(user, database) to choose backend when first message is received. */
  ClientSession(struct event_base* base, evutil_socket_t client_fd,
                const std::string& client_addr,
                pgpooler::config::BackendResolver resolver,
                pgpooler::config::PoolManager* pool_manager);
  ~ClientSession();

  ClientSession(const ClientSession&) = delete;
  ClientSession& operator=(const ClientSession&) = delete;

  /** Called from libevent callbacks (same TU only). */
  void on_client_read();
  void on_client_event(short what);
  void on_backend_read();
  void on_backend_event(short what);

  void flush_client_output();

  /** Called from raw client read event callback (same TU only). */
  void handle_client_read_event();

 private:
  void connect_to_backend();
  void on_backend_connected();
  void start_forwarding();
  void destroy();
  void schedule_flush_client();
  void send_error_and_close(const std::string& sqlstate, const std::string& message);

  struct event_base* base_ = nullptr;
  std::string backend_host_;
  std::uint16_t backend_port_ = 0;
  int session_id_ = 0;  // for logging
  std::string client_addr_;  // for routing / logging (e.g. "172.24.0.1")
  std::string backend_name_;
  pgpooler::config::PoolManager* pool_manager_ = nullptr;
  bool pool_acquired_ = false;
  pgpooler::config::BackendResolver resolver_;  // used once in connect_to_backend()

  evutil_socket_t client_fd_ = -1;
  struct evbuffer* client_input_ = nullptr;
  struct event* client_read_event_ = nullptr;
  struct bufferevent* bev_backend_ = nullptr;
  bool destroy_scheduled_ = false;  // guard against double destroy / double delete

  enum class State {
    ReadingFirst,         // waiting for first (startup or SSL request) from client
    ConnectingToBackend,  // non-blocking connect in progress
    WaitingSSLResponse,   // sent SSL request to backend, waiting for 1 byte 'S'/'N'
    Forwarding            // bidirectional relay
  };
  State state_ = State::ReadingFirst;

  std::vector<std::uint8_t> msg_buf_;
  std::vector<std::uint8_t> pending_startup_;  // first message, sent when backend connects
  /** Data to send to client (we write via send()). */
  std::vector<std::uint8_t> client_out_buf_;
};

}  // namespace session
}  // namespace pgpooler
