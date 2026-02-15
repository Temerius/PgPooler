#pragma once

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
 * State: read first (length-prefixed) message from client → connect to backend →
 * send first message → then forward messages both ways by protocol boundaries. */
class ClientSession {
 public:
  ClientSession(struct event_base* base, evutil_socket_t client_fd,
                const std::string& backend_host, std::uint16_t backend_port);
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

  struct event_base* base_ = nullptr;
  std::string backend_host_;
  std::uint16_t backend_port_ = 0;
  int session_id_ = 0;  // for logging

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
