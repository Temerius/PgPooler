#include "pool/connection_wait_queue.hpp"
#include "session/client_session.hpp"
#include <event2/event.h>

namespace pgpooler {
namespace pool {

ConnectionWaitQueue::ConnectionWaitQueue(struct event_base* base) : base_(base) {}

ConnectionWaitQueue::~ConnectionWaitQueue() {
  for (auto& w : waiters_) {
    if (w.timeout_ev) {
      event_del(w.timeout_ev);
      event_free(w.timeout_ev);
      w.timeout_ev = nullptr;
    }
  }
  waiters_.clear();
}

void ConnectionWaitQueue::on_timeout_cb(evutil_socket_t, short, void* ctx) {
  auto* w = static_cast<Waiter*>(ctx);
  ConnectionWaitQueue* queue = w->queue;
  session::ClientSession* session = w->session;
  if (!queue) return;
  for (auto it = queue->waiters_.begin(); it != queue->waiters_.end(); ++it) {
    if (&*it == w) {
      if (it->timeout_ev) {
        event_del(it->timeout_ev);
        event_free(it->timeout_ev);
        it->timeout_ev = nullptr;
      }
      queue->waiters_.erase(it);
      break;
    }
  }
  if (session) session->on_wait_timeout();
}

void ConnectionWaitQueue::enqueue(session::ClientSession* session,
                                  const std::string& backend_name,
                                  const std::string& user,
                                  const std::string& database,
                                  unsigned timeout_sec) {
  Waiter w;
  w.queue = this;
  w.session = session;
  w.backend_name = backend_name;
  w.user = user;
  w.database = database;
  struct timeval tv;
  tv.tv_sec = static_cast<long>(timeout_sec > 0 ? timeout_sec : 60);
  tv.tv_usec = 0;
  w.timeout_ev = event_new(base_, -1, 0, &ConnectionWaitQueue::on_timeout_cb, nullptr);
  if (!w.timeout_ev) return;
  waiters_.push_back(std::move(w));
  Waiter& back = waiters_.back();
  back.queue = this;
  event_assign(back.timeout_ev, base_, -1, 0, &ConnectionWaitQueue::on_timeout_cb, &back);
  event_add(back.timeout_ev, &tv);
}

void ConnectionWaitQueue::on_connection_available(const std::string& backend_name,
                                                  const std::string& user,
                                                  const std::string& database) {
  for (auto it = waiters_.begin(); it != waiters_.end(); ++it) {
    if (it->backend_name == backend_name && it->user == user && it->database == database) {
      session::ClientSession* session = it->session;
      if (it->timeout_ev) {
        event_del(it->timeout_ev);
        event_free(it->timeout_ev);
        it->timeout_ev = nullptr;
      }
      waiters_.erase(it);
      if (session) session->retry_connect_to_backend();
      return;
    }
  }
}

void ConnectionWaitQueue::remove(session::ClientSession* session) {
  for (auto it = waiters_.begin(); it != waiters_.end(); ++it) {
    if (it->session == session) {
      if (it->timeout_ev) {
        event_del(it->timeout_ev);
        event_free(it->timeout_ev);
        it->timeout_ev = nullptr;
      }
      waiters_.erase(it);
      return;
    }
  }
}

}  // namespace pool
}  // namespace pgpooler
