#pragma once

#include "config/config.hpp"
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <map>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <variant>
#include <vector>

struct pg_conn;

namespace pgpooler {
namespace analytics {

/** Event: connection started (after auth, when session goes to Forwarding). */
struct ConnectionStartEvent {
  int worker_id = -1;
  int session_id = 0;
  std::string client_addr;
  int client_port = 0;
  std::string username;
  std::string database_name;
  std::string backend_name;
  std::string pool_mode;  // "session" | "transaction" | "statement"
  std::string application_name;
};

/** Event: connection ended (disconnect). */
struct ConnectionEndEvent {
  int worker_id = -1;
  int session_id = 0;
  std::string disconnect_reason;  // "client_close" | "kill" | "timeout" | "error" | ""
};

/** Event: query started (client sent 'Q' Query message). */
struct QueryStartEvent {
  int worker_id = -1;
  int session_id = 0;
  std::string username;
  std::string database_name;
  std::string backend_name;
  std::string application_name;
  std::string query_text;
  std::chrono::steady_clock::time_point started_at;
};

/** Event: query finished (backend sent ReadyForQuery). */
struct QueryEndEvent {
  int worker_id = -1;
  int session_id = 0;
  std::chrono::steady_clock::time_point finished_at;
  double duration_ms = 0;
  std::string command_type;  // SELECT, INSERT, ...
  std::int64_t rows_affected = -1;
  std::int64_t rows_returned = -1;
  std::int64_t bytes_to_backend = 0;
  std::int64_t bytes_from_backend = 0;
  std::string error_sqlstate;
  std::string error_message;
};

/** Event: audit (connect, disconnect, kill, block, error, ...). */
struct AuditEvent {
  std::string event_type;   // "connect" | "disconnect" | "kill" | "block_user" | "config_reload" | "error"
  std::string severity;     // "info" | "warn" | "error"
  std::string username;
  std::string database_name;
  std::string backend_name;
  int session_id = -1;
  std::string client_addr;
  std::string message;
  std::string details_json;  // optional JSON for details
};

/** Event: close any remaining pending query rows (e.g. after error — no CommandComplete for failed statement). */
struct QueryFinalizeRemainingEvent {
  int worker_id = -1;
  int session_id = 0;
  std::string error_sqlstate;
  std::string error_message;
};

using AnalyticsEvent = std::variant<
  ConnectionStartEvent,
  ConnectionEndEvent,
  QueryStartEvent,
  QueryEndEvent,
  QueryFinalizeRemainingEvent,
  AuditEvent
>;

/** Non-blocking writer: push events to queue, background thread writes to analytics DB via libpq. */
class AnalyticsWriter {
 public:
  explicit AnalyticsWriter(const pgpooler::config::AnalyticsConfig& config);
  ~AnalyticsWriter();

  AnalyticsWriter(const AnalyticsWriter&) = delete;
  AnalyticsWriter& operator=(const AnalyticsWriter&) = delete;

  bool enabled() const { return config_.enabled && running_; }

  void push_connection_start(ConnectionStartEvent e);
  void push_connection_end(ConnectionEndEvent e);
  void push_query_start(QueryStartEvent e);
  void push_query_end(QueryEndEvent e);
  void push_query_finalize_remaining(QueryFinalizeRemainingEvent e);
  void push_audit(AuditEvent e);

 private:
  void writer_loop();
  bool connect();
  void process_one(const AnalyticsEvent& ev);
  bool exec_params(const char* sql, int n_params, const char* const* param_values);

  pgpooler::config::AnalyticsConfig config_;
  std::atomic<bool> running_{false};
  std::thread thread_;
  std::mutex queue_mutex_;
  std::queue<AnalyticsEvent> queue_;
  std::condition_variable cv_;

  struct pg_conn* conn_ = nullptr;
  std::mutex conn_mutex_;
  struct SessionState {
    int64_t connection_sessions_id = 0;
    std::vector<int64_t> pending_query_ids;  // INSERTed query row ids since last ReadyForQuery; all get updated on QueryEnd
  };
  std::map<std::pair<int, int>, SessionState> session_state_;
};

}  // namespace analytics
}  // namespace pgpooler
