#pragma once

#include "config/config.hpp"
#include <functional>
#include <optional>
#include <string>

struct event_base;

namespace pgpooler {
namespace pool {
class BackendConnectionPool;
class ConnectionWaitQueue;
}  // namespace pool
namespace analytics {
class AnalyticsWriter;
}  // namespace analytics
namespace server {

/** Schedule a periodic timer that pushes pool snapshots to analytics. Call from event loop thread.
 * interval_sec from config (0 = do not schedule). worker_id = -1 for single process. */
void schedule_pool_snapshot_timer(struct event_base* base,
                                  unsigned interval_sec,
                                  pgpooler::pool::BackendConnectionPool* connection_pool,
                                  pgpooler::pool::ConnectionWaitQueue* wait_queue,
                                  pgpooler::config::BackendResolver resolver,
                                  pgpooler::analytics::AnalyticsWriter* analytics,
                                  int worker_id);

}  // namespace server
}  // namespace pgpooler
