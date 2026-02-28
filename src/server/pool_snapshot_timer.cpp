#include "server/pool_snapshot_timer.hpp"
#include "analytics/analytics_writer.hpp"
#include "config/config.hpp"
#include "pool/backend_connection_pool.hpp"
#include "pool/connection_wait_queue.hpp"
#include <event2/event.h>
#include <map>
#include <set>
#include <string>
#include <tuple>

namespace pgpooler {
namespace server {

namespace {

static const char* pool_mode_str(config::PoolMode m) {
  using M = config::PoolMode;
  if (m == M::Session) return "session";
  if (m == M::Transaction) return "transaction";
  if (m == M::Statement) return "statement";
  return "session";
}

struct SnapshotCtx {
  pool::BackendConnectionPool* connection_pool = nullptr;
  pool::ConnectionWaitQueue* wait_queue = nullptr;
  config::BackendResolver resolver;
  analytics::AnalyticsWriter* analytics = nullptr;
  int worker_id = -1;
};

void on_snapshot_timer(evutil_socket_t, short, void* ctx) {
  auto* c = static_cast<SnapshotCtx*>(ctx);
  if (!c->analytics || !c->analytics->enabled()) return;
  if (!c->connection_pool || !c->wait_queue) return;

  auto pool_rows = c->connection_pool->get_snapshot();
  auto wait_rows = c->wait_queue->get_waiting_counts();

  std::map<std::tuple<std::string, std::string, std::string>, analytics::PoolSnapshotEvent::Row> key_to_row;
  for (const auto& r : pool_rows) {
    auto key = std::make_tuple(r.backend_name, r.user, r.database);
    analytics::PoolSnapshotEvent::Row row;
    row.backend_name = r.backend_name;
    row.username = r.user;
    row.database_name = r.database;
    row.pool_size = 0;
    row.pool_mode = "session";
    row.idle_count = r.idle_count;
    row.active_count = r.active_count;
    row.waiting_count = 0;
    key_to_row[key] = std::move(row);
  }
  for (const auto& w : wait_rows) {
    auto key = std::make_tuple(w.backend_name, w.user, w.database);
    auto it = key_to_row.find(key);
    if (it != key_to_row.end()) {
      it->second.waiting_count = w.count;
    } else {
      analytics::PoolSnapshotEvent::Row row;
      row.backend_name = w.backend_name;
      row.username = w.user;
      row.database_name = w.database;
      row.pool_size = 0;
      row.pool_mode = "session";
      row.idle_count = 0;
      row.active_count = 0;
      row.waiting_count = w.count;
      key_to_row[key] = std::move(row);
    }
  }

  for (auto& [key, row] : key_to_row) {
    auto opt = c->resolver(row.username, row.database_name);
    if (!opt) continue;
    if (opt->name != row.backend_name) continue;
    row.pool_size = opt->pool_size;
    row.pool_mode = pool_mode_str(opt->pool_mode);
  }

  analytics::PoolSnapshotEvent ev;
  ev.worker_id = c->worker_id;
  for (auto& [_, row] : key_to_row) {
    ev.rows.push_back(std::move(row));
  }
  if (!ev.rows.empty()) {
    c->analytics->push_pool_snapshot(std::move(ev));
  }
}

}  // namespace

void schedule_pool_snapshot_timer(struct event_base* base,
                                  unsigned interval_sec,
                                  pool::BackendConnectionPool* connection_pool,
                                  pool::ConnectionWaitQueue* wait_queue,
                                  config::BackendResolver resolver,
                                  analytics::AnalyticsWriter* analytics,
                                  int worker_id) {
  if (interval_sec == 0) return;
  if (!analytics || !analytics->enabled()) return;
  if (!connection_pool || !wait_queue) return;

  auto* ctx = new SnapshotCtx;
  ctx->connection_pool = connection_pool;
  ctx->wait_queue = wait_queue;
  ctx->resolver = std::move(resolver);
  ctx->analytics = analytics;
  ctx->worker_id = worker_id;

  struct timeval tv;
  tv.tv_sec = static_cast<long>(interval_sec);
  tv.tv_usec = 0;
  event* ev = event_new(base, -1, EV_PERSIST, on_snapshot_timer, ctx);
  if (!ev) {
    delete ctx;
    return;
  }
  event_add(ev, &tv);
}

}  // namespace server
}  // namespace pgpooler
