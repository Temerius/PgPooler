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

/** Remove SQL comments: single-line (--) and block (slash-star to star-slash); respect string literals. */
std::string strip_sql_comments(const std::string& query) {
  std::string out;
  out.reserve(query.size());
  bool in_single = false;
  bool in_double = false;
  const size_t n = query.size();
  size_t i = 0;
  while (i < n) {
    unsigned char c = static_cast<unsigned char>(query[i]);
    if (in_single) {
      if (c == '\'') {
        if (i + 1 < n && query[i + 1] == '\'') { out += '\''; out += '\''; i += 2; continue; }
        in_single = false;
        out += '\'';
        i++;
        continue;
      }
      out += static_cast<char>(c);
      i++;
      continue;
    }
    if (in_double) {
      if (c == '"') {
        in_double = false;
        out += '"';
        i++;
        continue;
      }
      out += static_cast<char>(c);
      i++;
      continue;
    }
    if (c == '\'' && !in_double) {
      in_single = true;
      out += '\'';
      i++;
      continue;
    }
    if (c == '"' && !in_single) {
      in_double = true;
      out += '"';
      i++;
      continue;
    }
    if (c == '-' && i + 1 < n && query[i + 1] == '-') {
      i += 2;
      while (i < n && query[i] != '\n' && query[i] != '\r') i++;
      continue;
    }
    if (c == '/' && i + 1 < n && query[i + 1] == '*') {
      i += 2;
      while (i + 1 < n && !(query[i] == '*' && query[i + 1] == '/')) i++;
      if (i + 1 < n) i += 2;
      continue;
    }
    out += static_cast<char>(c);
    i++;
  }
  return out;
}

/** Normalize query text to a fingerprint: strip comments, replace literals/numbers with ?, uppercase. No space/whitespace changes. */
std::string normalize_query_fingerprint(const std::string& query) {
  std::string q = strip_sql_comments(query);
  std::string out;
  out.reserve(q.size());
  bool in_single = false;
  bool in_double = false;
  const size_t n = q.size();
  size_t i = 0;
  while (i < n) {
    unsigned char c = static_cast<unsigned char>(q[i]);
    if (in_single) {
      if (c == '\'') {
        if (i + 1 < n && q[i + 1] == '\'') { i += 2; continue; }
        in_single = false;
        out += '?';
        i++;
        continue;
      }
      i++;
      continue;
    }
    if (in_double) {
      if (c == '"') {
        in_double = false;
        out += '?';
        i++;
        continue;
      }
      i++;
      continue;
    }
    if (c == '\'' && !in_double) {
      in_single = true;
      i++;
      continue;
    }
    if (c == '"' && !in_single) {
      in_double = true;
      i++;
      continue;
    }
    /* Standalone number -> ? (not part of identifier) */
    if (c >= '0' && c <= '9') {
      bool standalone = (out.empty() || out.back() == ' ' || out.back() == '\t' || out.back() == '\n' || out.back() == '\r' ||
                        out.back() == '(' || out.back() == ',' || out.back() == '=' || out.back() == '?' || out.back() == '<' || out.back() == '>');
      if (standalone) {
        while (i < n && q[i] >= '0' && q[i] <= '9') i++;
        if (i < n && q[i] == '.' && i + 1 < n && q[i + 1] >= '0' && q[i + 1] <= '9') {
          i++;
          while (i < n && q[i] >= '0' && q[i] <= '9') i++;
        }
        out += '?';
        continue;
      }
    }
    out += static_cast<char>(c);
    i++;
  }
  if (in_single || in_double) out += '?';
  /* Trim leading and trailing whitespace */
  size_t start = 0;
  while (start < out.size()) {
    unsigned char u = static_cast<unsigned char>(out[start]);
    if (u != ' ' && u != '\t' && u != '\n' && u != '\r') break;
    start++;
  }
  size_t end = out.size();
  while (end > start) {
    unsigned char u = static_cast<unsigned char>(out[end - 1]);
    if (u != ' ' && u != '\t' && u != '\n' && u != '\r') break;
    end--;
  }
  if (start > 0 || end < out.size()) out = out.substr(start, end - start);
  if (out.size() > 10000) out = out.substr(0, 10000);
  for (size_t j = 0; j < out.size(); ++j) {
    unsigned char u = static_cast<unsigned char>(out[j]);
    if (u >= 'a' && u <= 'z') out[j] = static_cast<char>(u - 32);
  }
  return out;
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

void AnalyticsWriter::push_update_session_application_name(UpdateSessionApplicationNameEvent e) {
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

int64_t AnalyticsWriter::get_or_create_fingerprint_id(const std::string& fingerprint) {
  if (fingerprint.empty() || !conn_) return 0;
  /* SHA-256: 32 bytes. Requires pgcrypto. Partial unique index: must repeat its WHERE in ON CONFLICT. */
  const char* sql = "INSERT INTO pgpooler.query_fingerprints (fingerprint, fingerprint_hash) "
      "VALUES ($1, digest($1, 'sha256')) "
      "ON CONFLICT (fingerprint_hash) WHERE (fingerprint_hash IS NOT NULL) "
      "DO UPDATE SET fingerprint = pgpooler.query_fingerprints.fingerprint "
      "RETURNING id";
  const char* vals[1] = { fingerprint.c_str() };
  PGresult* res = PQexecParams(conn_, sql, 1, nullptr, vals, nullptr, nullptr, 0);
  if (!res || PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) != 1) {
    if (res) {
      pgpooler::log::warn("analytics: query_fingerprints insert failed: " + std::string(PQerrorMessage(conn_)));
      PQclear(res);
    }
    return 0;
  }
  int64_t id = std::stoll(PQgetvalue(res, 0, 0));
  PQclear(res);
  pgpooler::log::debug("analytics: query_fingerprints id=" + std::to_string(id) + " fingerprint=" + fingerprint.substr(0, 60) + (fingerprint.size() > 60 ? "..." : ""));
  return id;
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
      std::string fingerprint = normalize_query_fingerprint(arg.query_text);
      std::lock_guard<std::mutex> lock(conn_mutex_);
      if (!conn_) {
        pgpooler::log::warn("analytics: INSERT queries skipped (no connection)");
        return;
      }
      int64_t fingerprint_id = fingerprint.empty() ? 0 : get_or_create_fingerprint_id(fingerprint);
      const char* sql = "INSERT INTO pgpooler.queries "
          "(connection_session_id, session_id, username, database_name, backend_name, query_text, query_text_length, fingerprint_id, started_at) "
          "VALUES ($1::bigint, $2::int, $3, $4, $5, $6, $7::int, $8::bigint, now()) "
          "RETURNING id";
      std::string cid_str = std::to_string(cid);
      std::string sid_str = std::to_string(arg.session_id);
      std::string len_str = std::to_string(static_cast<int>(arg.query_text.size()));
      std::string fid_str = fingerprint_id > 0 ? std::to_string(fingerprint_id) : "";
      const char* fid_ptr = fingerprint_id > 0 ? fid_str.c_str() : nullptr;
      const char* vals[8] = {
        cid_str.c_str(), sid_str.c_str(),
        arg.username.c_str(),
        arg.database_name.c_str(),
        arg.backend_name.c_str(),
        qtext.c_str(),
        len_str.c_str(),
        fid_ptr
      };
      PGresult* res = PQexecParams(conn_, sql, 8, nullptr, vals, nullptr, nullptr, 0);
      if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) == 1) {
        int64_t qid = std::stoll(PQgetvalue(res, 0, 0));
        session_state_[{arg.worker_id, arg.session_id}].pending_query_ids.push_back(qid);
        pgpooler::log::info("analytics: INSERT queries ok id=" + std::to_string(qid));
        /* Keep connection_sessions.application_name in sync (call PQexecParams directly — we already hold conn_mutex_). */
        if (!arg.application_name.empty() && conn_) {
          std::string wid_str = std::to_string(arg.worker_id);
          std::string sid_str = std::to_string(arg.session_id);
          const char* sql_app = "UPDATE pgpooler.connection_sessions SET application_name = NULLIF($1,'') WHERE session_id = $2::int AND worker_id = $3::int AND disconnected_at IS NULL";
          const char* vals_app[3] = { arg.application_name.c_str(), sid_str.c_str(), wid_str.c_str() };
          PGresult* res_app = PQexecParams(conn_, sql_app, 3, nullptr, vals_app, nullptr, nullptr, 0);
          if (res_app) PQclear(res_app);
        }
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
      std::string ra_str = arg.rows_affected >= 0 ? std::to_string(arg.rows_affected) : "";
      std::string rr_str = arg.rows_returned >= 0 ? std::to_string(arg.rows_returned) : "";
      std::string bt_str = std::to_string(arg.bytes_to_backend);
      std::string bf_str = std::to_string(arg.bytes_from_backend);
      const char* ra_ptr = arg.rows_affected >= 0 ? ra_str.c_str() : nullptr;
      const char* rr_ptr = arg.rows_returned >= 0 ? rr_str.c_str() : nullptr;
      const char* sql = "UPDATE pgpooler.queries SET finished_at = now(), "
          "command_type = NULLIF($2,''), rows_affected = $3::bigint, rows_returned = $4::bigint, "
          "bytes_to_backend = $5::bigint, bytes_from_backend = $6::bigint, "
          "error_sqlstate = NULLIF($7,''), error_message = NULLIF($8,'') "
          "WHERE id = $1::bigint";
      const char* vals[8] = {
        qid_str.c_str(), arg.command_type.empty() ? "" : arg.command_type.c_str(),
        ra_ptr, rr_ptr, bt_str.c_str(), bf_str.c_str(),
        arg.error_sqlstate.empty() ? "" : arg.error_sqlstate.c_str(),
        arg.error_message.empty() ? "" : arg.error_message.c_str()
      };
      if (exec_params(sql, 8, vals)) {
        pgpooler::log::info("analytics: UPDATE queries (end) ok id=" + std::to_string(qid));
      }
    } else if constexpr (std::is_same_v<T, UpdateSessionApplicationNameEvent>) {
      /* application_name only in connection_sessions (not in queries). */
      std::string wid_str = std::to_string(arg.worker_id);
      std::string sid_str = std::to_string(arg.session_id);
      const char* sql_conn = "UPDATE pgpooler.connection_sessions SET application_name = NULLIF($1,'') WHERE session_id = $2::int AND worker_id = $3::int AND disconnected_at IS NULL";
      const char* vals_conn[3] = { arg.application_name.c_str(), sid_str.c_str(), wid_str.c_str() };
      if (exec_params(sql_conn, 3, vals_conn)) {
        pgpooler::log::info("analytics: UPDATE connection_sessions application_name ok session_id=" + sid_str + " worker_id=" + wid_str);
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
      for (size_t i = 0; i < ids.size(); ++i) {
        const int64_t qid = ids[i];
        const std::int64_t bt = (i < arg.bytes_to_backend.size()) ? arg.bytes_to_backend[i] : 0;
        std::string qid_str = std::to_string(qid);
        std::string bt_str = std::to_string(bt);
        const char* sql = "UPDATE pgpooler.queries SET finished_at = now(), "
            "command_type = upper(split_part(trim(regexp_replace(query_text, E'[\\s\\n\\r]+', ' ', 'g')), ' ', 1)), "
            "bytes_to_backend = $2::bigint, error_sqlstate = NULLIF($3,''), error_message = NULLIF($4,'') WHERE id = $1::bigint";
        const char* vals[4] = { qid_str.c_str(), bt_str.c_str(), arg.error_sqlstate.c_str(), arg.error_message.c_str() };
        if (exec_params(sql, 4, vals)) {
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
