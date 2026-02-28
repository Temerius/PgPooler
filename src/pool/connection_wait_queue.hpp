#pragma once

#include <event2/event.h>
#include <event2/util.h>
#include <cstdint>
#include <list>
#include <string>
#include <vector>

struct event_base;

namespace pgpooler {
namespace session {
class ClientSession;
}
namespace pool {

/** One row for waiting count per (backend, user, database). */
struct WaitingCountRow {
  std::string backend_name;
  std::string user;
  std::string database;
  unsigned count = 0;
};

/** Per-backend wait queue when pool is full. Call from event loop thread only. */
class ConnectionWaitQueue {
 public:
  explicit ConnectionWaitQueue(struct event_base* base);
  ~ConnectionWaitQueue();

  ConnectionWaitQueue(const ConnectionWaitQueue&) = delete;
  ConnectionWaitQueue& operator=(const ConnectionWaitQueue&) = delete;

  /** Current waiting count per (backend_name, user, database). Call from event loop thread. */
  std::vector<WaitingCountRow> get_waiting_counts() const;

  /** Enqueue session to wait for a connection. Schedules timeout. */
  void enqueue(session::ClientSession* session,
               const std::string& backend_name,
               const std::string& user,
               const std::string& database,
               unsigned timeout_sec);

  /** Wake one waiter for (backend_name, user, database). Call after putting a connection in the pool. */
  void on_connection_available(const std::string& backend_name,
                               const std::string& user,
                               const std::string& database);

  /** Remove session from queue (e.g. on destroy). */
  void remove(session::ClientSession* session);

 private:
  struct Waiter {
    ConnectionWaitQueue* queue = nullptr;
    session::ClientSession* session = nullptr;
    std::string backend_name;
    std::string user;
    std::string database;
    struct event* timeout_ev = nullptr;
  };
  static void on_timeout_cb(evutil_socket_t, short, void* ctx);

  struct event_base* base_ = nullptr;
  std::list<Waiter> waiters_;
};

}  // namespace pool
}  // namespace pgpooler
