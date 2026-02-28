#include "analytics/analytics_writer.hpp"
#include "common/log.hpp"
#include <libpq-fe.h>
#include <cstring>
#include <sstream>

namespace pgpooler {
namespace analytics {

namespace {

std::string conninfo(const pgpooler::config::AnalyticsConfig& c) {
  std::ostringstream o;
  o << "host=" << c.host << " port=" << c.port
    << " user=" << c.user << " dbname=" << c.dbname;
  if (!c.password.empty()) o << " password=" << c.password;
  return o.str();
}

}  // namespace

AnalyticsWriter::AnalyticsWriter(const pgpooler::config::AnalyticsConfig& config)
    : config_(config) {
  if (!config_.enabled) {
    pgpooler::log::info("analytics: disabled in config");
    return;
  }
  pgpooler::log::info("analytics: enabling writer dbname=" + config_.dbname + " host=" + config_.host);
  running_ = true;
  thread_ = std::thread(&AnalyticsWriter::writer_loop, this);
  pgpooler::log::info("analytics: writer thread started");
}

AnalyticsWriter::~AnalyticsWriter() {
  if (!config_.enabled) return;
  running_ = false;
  cv_.notify_all();
  if (thread_.joinable()) thread_.join();
  if (conn_) {
    PQfinish(conn_);
    conn_ = nullptr;
  }
  pgpooler::log::info("analytics: writer thread stopped");
}

void AnalyticsWriter::push_connection_start(ConnectionStartEvent e) {
  if (!config_.enabled) return;
  pgpooler::log::debug("analytics: push connection_start worker_id=" + std::to_string(e.worker_id) +
                       " session_id=" + std::to_string(e.session_id) + " backend=" + e.backend_name);
  std::lock_guard<std::mutex> lock(queue_mutex_);
  queue_.push(e);
  cv_.notify_one();
}

void AnalyticsWriter::push_connection_end(ConnectionEndEvent e) {
  if (!config_.enabled) return;
  pgpooler::log::debug("analytics: push connection_end worker_id=" + std::to_string(e.worker_id) +
                       " session_id=" + std::to_string(e.session_id) + " reason=" + e.disconnect_reason);
  std::lock_guard<std::mutex> lock(queue_mutex_);
  queue_.push(e);
  cv_.notify_one();
}

void AnalyticsWriter::push_query_start(QueryStartEvent e) {
  if (!config_.enabled) return;
  pgpooler::log::debug("analytics: push query_start worker_id=" + std::to_string(e.worker_id) +
                       " session_id=" + std::to_string(e.session_id) + " query_len=" + std::to_string(e.query_text.size()));
  std::lock_guard<std::mutex> lock(queue_mutex_);
  queue_.push(e);
  cv_.notify_one();
}

void AnalyticsWriter::push_query_end(QueryEndEvent e) {
  if (!config_.enabled) return;
  pgpooler::log::debug("analytics: push query_end worker_id=" + std::to_string(e.worker_id) +
                       " session_id=" + std::to_string(e.session_id) + " duration_ms=" + std::to_string(static_cast<long long>(e.duration_ms)));
  std::lock_guard<std::mutex> lock(queue_mutex_);
  queue_.push(e);
  cv_.notify_one();
}

void AnalyticsWriter::push_query_finalize_remaining(QueryFinalizeRemainingEvent e) {
  if (!config_.enabled) return;
  std::lock_guard<std::mutex> lock(queue_mutex_);
  queue_.push(e);
  cv_.notify_one();
}

void AnalyticsWriter::push_audit(AuditEvent e) {
  if (!config_.enabled) return;
  std::lock_guard<std::mutex> lock(queue_mutex_);
  queue_.push(e);
  cv_.notify_one();
}

void AnalyticsWriter::writer_loop() {
  while (running_) {
    {
      std::unique_lock<std::mutex> lock(queue_mutex_);
      cv_.wait_for(lock, std::chrono::milliseconds(100), [this] {
        return !running_ || !queue_.empty();
      });
      if (!running_) break;
      if (queue_.empty()) continue;
    }
    /* Do not pop until we have a connection, so we never drop events when DB is unreachable. */
    if (!conn_ && !connect()) {
      pgpooler::log::warn("analytics: connect failed, will retry on next wakeup");
      continue;
    }
    AnalyticsEvent ev;
    {
      std::lock_guard<std::mutex> lock(queue_mutex_);
      if (queue_.empty()) continue;
      ev = std::move(queue_.front());
      queue_.pop();
    }
    process_one(ev);
  }
}

bool AnalyticsWriter::connect() {
  std::lock_guard<std::mutex> lock(conn_mutex_);
  if (conn_) return PQstatus(conn_) == CONNECTION_OK;
  conn_ = PQconnectdb(conninfo(config_).c_str());
  if (PQstatus(conn_) != CONNECTION_OK) {
    pgpooler::log::warn("analytics: " + std::string(PQerrorMessage(conn_)));
    PQfinish(conn_);
    conn_ = nullptr;
    return false;
  }
  return true;
}

bool AnalyticsWriter::exec_params(const char* sql, int n_params, const char* const* param_values) {
  std::lock_guard<std::mutex> lock(conn_mutex_);
  if (!conn_ || PQstatus(conn_) != CONNECTION_OK) return false;
  PGresult* res = PQexecParams(conn_, sql, n_params, nullptr, param_values, nullptr, nullptr, 0);
  if (PQresultStatus(res) != PGRES_COMMAND_OK && PQresultStatus(res) != PGRES_TUPLES_OK) {
    pgpooler::log::warn("analytics: " + std::string(PQerrorMessage(conn_)));
    PQclear(res);
    return false;
  }
  PQclear(res);
  return true;
}

void AnalyticsWriter::process_one(const AnalyticsEvent& ev) {
  std::visit([this](auto&& arg) {
    using T = std::decay_t<decltype(arg)>;
    if constexpr (std::is_same_v<T, ConnectionStartEvent>) {
      pgpooler::log::debug("analytics: INSERT connection_sessions worker_id=" + std::to_string(arg.worker_id) +
                           " session_id=" + std::to_string(arg.session_id));
      const char* sql = "INSERT INTO pgpooler.connection_sessions "
          "(session_id, worker_id, client_addr, client_port, username, database_name, backend_name, pool_mode, application_name) "
          "VALUES ($1::int, $2::int, NULLIF($3,'')::inet, NULLIF($4,'')::int, $5, $6, $7, $8, NULLIF($9,'')) "
          "RETURNING id";
      std::string s_sid = std::to_string(arg.session_id);
      std::string s_wid = std::to_string(arg.worker_id);
      std::string s_port = arg.client_port > 0 ? std::to_string(arg.client_port) : "";
      const char* vals[9] = {
        s_sid.c_str(), s_wid.c_str(),
        arg.client_addr.empty() ? "" : arg.client_addr.c_str(),
        s_port.c_str(),
        arg.username.c_str(), arg.database_name.c_str(), arg.backend_name.c_str(),
        arg.pool_mode.c_str(),
        arg.application_name.empty() ? "" : arg.application_name.c_str()
      };
      std::lock_guard<std::mutex> lock(conn_mutex_);
      if (!conn_) {
        pgpooler::log::warn("analytics: INSERT connection_sessions skipped (no connection)");
        return;
      }
      PGresult* res = PQexecParams(conn_, sql, 9, nullptr, vals, nullptr, nullptr, 0);
      if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) == 1) {
        int64_t id = std::stoll(PQgetvalue(res, 0, 0));
        session_state_[{arg.worker_id, arg.session_id}].connection_sessions_id = id;
        pgpooler::log::info("analytics: INSERT connection_sessions ok id=" + std::to_string(id));
      } else {
        pgpooler::log::warn("analytics: INSERT connection_sessions failed: " + std::string(PQerrorMessage(conn_)));
      }
      if (res) PQclear(res);
    } else if constexpr (std::is_same_v<T, ConnectionEndEvent>) {
      int64_t cid = 0;
      {
        std::lock_guard<std::mutex> lock(conn_mutex_);
        auto it = session_state_.find({arg.worker_id, arg.session_id});
        if (it == session_state_.end()) {
          pgpooler::log::warn("analytics: UPDATE connection_sessions (end) skipped: no session_state worker_id=" +
                              std::to_string(arg.worker_id) + " session_id=" + std::to_string(arg.session_id));
          return;
        }
        cid = it->second.connection_sessions_id;
        session_state_.erase(it);
      }
      pgpooler::log::debug("analytics: UPDATE connection_sessions SET disconnected_at id=" + std::to_string(cid));
      const char* sql = "UPDATE pgpooler.connection_sessions SET disconnected_at = now(), "
          "duration_sec = EXTRACT(EPOCH FROM (now() - connected_at)), disconnect_reason = NULLIF($2,'') "
          "WHERE id = $1::bigint";
      std::string cid_str = std::to_string(cid);
      const char* vals[2] = { cid_str.c_str(), arg.disconnect_reason.c_str() };
      if (exec_params(sql, 2, vals)) {
        pgpooler::log::info("analytics: UPDATE connection_sessions (end) ok id=" + std::to_string(cid));
      }
    } else if constexpr (std::is_same_v<T, QueryStartEvent>) {
      int64_t cid = 0;
      {
        std::lock_guard<std::mutex> lock(conn_mutex_);
        auto it = session_state_.find({arg.worker_id, arg.session_id});
        if (it == session_state_.end()) {
          pgpooler::log::warn("analytics: INSERT queries skipped: no session_state worker_id=" +
                              std::to_string(arg.worker_id) + " session_id=" + std::to_string(arg.session_id));
          return;
        }
        cid = it->second.connection_sessions_id;
      }
      pgpooler::log::debug("analytics: INSERT queries connection_session_id=" + std::to_string(cid));
      std::string qtext = arg.query_text;
      if (qtext.size() > 10000) qtext = qtext.substr(0, 10000);
      const char* sql = "INSERT INTO pgpooler.queries "
          "(connection_session_id, session_id, username, database_name, backend_name, application_name, query_text, query_text_length, started_at) "
          "VALUES ($1::bigint, $2::int, $3, $4, $5, NULLIF($6,''), $7, $8::int, now()) "
          "RETURNING id";
      std::string cid_str = std::to_string(cid);
      std::string sid_str = std::to_string(arg.session_id);
      std::string len_str = std::to_string(static_cast<int>(arg.query_text.size()));
      const char* vals[8] = {
        cid_str.c_str(), sid_str.c_str(),
        arg.username.c_str(),
        arg.database_name.c_str(),
        arg.backend_name.c_str(),
        arg.application_name.empty() ? "" : arg.application_name.c_str(),
        qtext.c_str(),
        len_str.c_str()
      };
      std::lock_guard<std::mutex> lock(conn_mutex_);
      if (!conn_) {
        pgpooler::log::warn("analytics: INSERT queries skipped (no connection)");
        return;
      }
      PGresult* res = PQexecParams(conn_, sql, 8, nullptr, vals, nullptr, nullptr, 0);
      if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) == 1) {
        int64_t qid = std::stoll(PQgetvalue(res, 0, 0));
        session_state_[{arg.worker_id, arg.session_id}].pending_query_ids.push_back(qid);
        pgpooler::log::info("analytics: INSERT queries ok id=" + std::to_string(qid));
      } else {
        pgpooler::log::warn("analytics: INSERT queries failed: " + std::string(PQerrorMessage(conn_)));
      }
      if (res) PQclear(res);
    } else if constexpr (std::is_same_v<T, QueryEndEvent>) {
      int64_t qid = 0;
      {
        std::lock_guard<std::mutex> lock(conn_mutex_);
        auto it = session_state_.find({arg.worker_id, arg.session_id});
        if (it == session_state_.end()) {
          pgpooler::log::warn("analytics: UPDATE queries (end) skipped: no session_state worker_id=" +
                              std::to_string(arg.worker_id) + " session_id=" + std::to_string(arg.session_id));
          return;
        }
        if (it->second.pending_query_ids.empty()) {
          pgpooler::log::warn("analytics: UPDATE queries (end) skipped: no pending_query_ids");
          return;
        }
        qid = it->second.pending_query_ids.front();
        it->second.pending_query_ids.erase(it->second.pending_query_ids.begin());
      }
      pgpooler::log::debug("analytics: UPDATE queries SET finished_at id=" + std::to_string(qid));
      std::string qid_str = std::to_string(qid);
      std::string dur_str = std::to_string(arg.duration_ms);
      std::string ra_str = arg.rows_affected >= 0 ? std::to_string(arg.rows_affected) : "";
      std::string rr_str = arg.rows_returned >= 0 ? std::to_string(arg.rows_returned) : "";
      std::string bt_str = std::to_string(arg.bytes_to_backend);
      std::string bf_str = std::to_string(arg.bytes_from_backend);
      const char* ra_ptr = arg.rows_affected >= 0 ? ra_str.c_str() : nullptr;
      const char* rr_ptr = arg.rows_returned >= 0 ? rr_str.c_str() : nullptr;
      const char* sql = "UPDATE pgpooler.queries SET finished_at = now(), duration_ms = $2::numeric, "
          "command_type = NULLIF($3,''), rows_affected = $4::bigint, rows_returned = $5::bigint, "
          "bytes_to_backend = $6::bigint, bytes_from_backend = $7::bigint, "
          "error_sqlstate = NULLIF($8,''), error_message = NULLIF($9,'') "
          "WHERE id = $1::bigint";
      const char* vals[9] = {
        qid_str.c_str(), dur_str.c_str(), arg.command_type.empty() ? "" : arg.command_type.c_str(),
        ra_ptr, rr_ptr, bt_str.c_str(), bf_str.c_str(),
        arg.error_sqlstate.empty() ? "" : arg.error_sqlstate.c_str(),
        arg.error_message.empty() ? "" : arg.error_message.c_str()
      };
      if (exec_params(sql, 9, vals)) {
        pgpooler::log::info("analytics: UPDATE queries (end) ok id=" + std::to_string(qid));
      }
    } else if constexpr (std::is_same_v<T, QueryFinalizeRemainingEvent>) {
      std::vector<int64_t> ids;
      {
        std::lock_guard<std::mutex> lock(conn_mutex_);
        auto it = session_state_.find({arg.worker_id, arg.session_id});
        if (it == session_state_.end()) return;
        ids = std::move(it->second.pending_query_ids);
        it->second.pending_query_ids.clear();
      }
      for (int64_t qid : ids) {
        std::string qid_str = std::to_string(qid);
        const char* sql = "UPDATE pgpooler.queries SET finished_at = now(), duration_ms = 0, "
            "error_sqlstate = NULLIF($2,''), error_message = NULLIF($3,'') WHERE id = $1::bigint";
        const char* vals[3] = { qid_str.c_str(), arg.error_sqlstate.c_str(), arg.error_message.c_str() };
        if (exec_params(sql, 3, vals)) {
          pgpooler::log::info("analytics: UPDATE queries (finalize remaining) ok id=" + std::to_string(qid));
        }
      }
    } else if constexpr (std::is_same_v<T, AuditEvent>) {
      const char* sql = "INSERT INTO pgpooler.events (event_type, severity, username, database_name, backend_name, session_id, client_addr, message, details) "
          "VALUES ($1, NULLIF($2,''), NULLIF($3,''), NULLIF($4,''), NULLIF($5,''), $6::int, NULLIF($7,'')::inet, NULLIF($8,''), $9::jsonb)";
      std::string sid_str = arg.session_id >= 0 ? std::to_string(arg.session_id) : "";
      const char* sid_ptr = arg.session_id >= 0 ? sid_str.c_str() : nullptr;
      std::string details = arg.details_json.empty() ? "null" : arg.details_json;
      const char* vals[9] = {
        arg.event_type.c_str(), arg.severity.c_str(), arg.username.c_str(),
        arg.database_name.c_str(), arg.backend_name.c_str(), sid_ptr,
        arg.client_addr.c_str(), arg.message.c_str(), details.c_str()
      };
      exec_params(sql, 9, vals);
    }
  }, ev);
}

}  // namespace analytics
}  // namespace pgpooler
