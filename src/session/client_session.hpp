#pragma once

#include "config/config.hpp"
#include <event2/util.h>
#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

struct event_base;
struct bufferevent;
struct event;
struct evbuffer;

namespace pgpooler {
namespace pool {
class BackendConnectionPool;
class ConnectionWaitQueue;
}
namespace session {

/** Holds client connection and proxies to a single PostgreSQL backend.
 * State: read first (length-prefixed) message from client → resolve backend →
 * connect to backend → send first message → then forward messages both ways. */
class ClientSession {
 public:
  /** If initial_data is non-empty, it is pushed into client input and on_client_read is scheduled once (for fd handoff from dispatcher). worker_id for log prefix (-1 if not from worker). */
  ClientSession(struct event_base* base, evutil_socket_t client_fd,
                const std::string& client_addr,
                pgpooler::config::BackendResolver resolver,
                pgpooler::config::PoolManager* pool_manager,
                pgpooler::pool::BackendConnectionPool* connection_pool,
                pgpooler::pool::ConnectionWaitQueue* wait_queue,
                const std::vector<std::uint8_t>* initial_data = nullptr,
                int worker_id = -1);
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

  /** Called by ConnectionWaitQueue when a connection becomes available. */
  void retry_connect_to_backend();
  /** Called by ConnectionWaitQueue when wait timeout expires. */
  void on_wait_timeout();

  /** Called from client write event callback (same TU only): flush then maybe do_return_backend_to_pool. */
  void on_client_writable();

  /** One-shot callback for deferred destroy (must not destroy from inside flush/on_backend_read). */
  static void static_deferred_destroy_cb(evutil_socket_t, short, void* ctx);

  enum class State {
    ReadingFirst,
    ConnectingToBackend,
    WaitingSSLResponse,
    CollectingStartupResponse,
    SendingDiscardAll,
    Forwarding,
    WaitingForBackend
  };

 private:
  void connect_to_backend();
  void on_backend_connected();
  void start_forwarding();
  void return_backend_to_pool();
  void do_return_backend_to_pool();  // actual put, called when client_out_buf_ empty
  void destroy();
  void schedule_flush_client();
  void send_error_and_close(const std::string& sqlstate, const std::string& message);
  void forward_client_to_backend();
  /** Close the current backend without returning it to the pool (e.g. auth-only connection). */
  void close_auth_backend();

  struct event_base* base_ = nullptr;
  std::string backend_host_;
  std::uint16_t backend_port_ = 0;
  int session_id_ = 0;
  std::string client_addr_;
  std::string backend_name_;
  std::string user_;
  std::string database_;
  pgpooler::config::PoolMode pool_mode_ = pgpooler::config::PoolMode::Session;
  unsigned server_idle_timeout_sec_ = 0;
  unsigned server_lifetime_sec_ = 0;
  unsigned query_wait_timeout_sec_ = 0;
  std::chrono::steady_clock::time_point backend_created_at_{std::chrono::steady_clock::now()};
  pgpooler::config::PoolManager* pool_manager_ = nullptr;
  pgpooler::pool::ConnectionWaitQueue* wait_queue_ = nullptr;
  bool waiting_in_queue_ = false;
  pgpooler::pool::BackendConnectionPool* connection_pool_ = nullptr;
  bool pool_acquired_ = false;
  pgpooler::config::BackendResolver resolver_;

  int worker_id_ = -1;
  evutil_socket_t client_fd_ = -1;
  struct evbuffer* client_input_ = nullptr;
  struct event* client_read_event_ = nullptr;
  struct event* client_write_event_ = nullptr;  // when we need to flush before return to pool
  struct bufferevent* bev_backend_ = nullptr;
  bool destroy_scheduled_ = false;  // guard against double destroy / double delete
  bool deferred_destroy_pending_ = false;  // flush failed, destroy scheduled for next tick (must not delete inside callback)
  bool pending_return_to_pool_ = false;  // waiting for client_out_buf_ to drain before put
  bool backend_dead_ = false;  // backend eof/error: do not put connection back to pool

  State state_ = State::ReadingFirst;

  std::vector<std::uint8_t> msg_buf_;
  std::vector<std::uint8_t> pending_startup_;
  std::vector<std::uint8_t> client_startup_cache_;
  std::vector<std::uint8_t> cached_startup_response_;
  std::vector<std::uint8_t> client_out_buf_;
};

}  // namespace session
}  // namespace pgpooler
