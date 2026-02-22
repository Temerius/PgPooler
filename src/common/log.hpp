#pragma once

#include <chrono>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

namespace pgpooler {
namespace config { struct LoggingConfig; }
namespace log {

/** Log levels: 0=error, 1=warn, 2=info, 3=debug. Default 2 (info). */
inline int& level() {
  static int value = 2;
  return value;
}

/** Set level from config string: "error", "warn", "info", "debug". Unknown -> info. */
inline void set_level(const std::string& s) {
  if (s == "error") level() = 0;
  else if (s == "warn") level() = 1;
  else if (s == "info") level() = 2;
  else if (s == "debug") level() = 3;
  else level() = 2;
}

/** Initialize logging from config (file only, no stderr): level, path/directory+filename, append. */
void init(const config::LoggingConfig& cfg);

/** Set log output to file. append: true = append, false = overwrite. */
void set_log_file(const std::string& path, bool append = true);

namespace detail {

inline std::string timestamp() {
  auto now = std::chrono::system_clock::now();
  auto t = std::chrono::system_clock::to_time_t(now);
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                now.time_since_epoch()) % 1000;
  std::ostringstream os;
  os << std::put_time(std::localtime(&t), "%Y-%m-%d %H:%M:%S");
  os << '.' << std::setfill('0') << std::setw(3) << ms.count();
  return os.str();
}

std::ostream& log_stream();

inline void write(const char* level_name, int session_id, const std::string& msg) {
  std::ostream& out = log_stream();
  out << "[" << timestamp() << "] [" << level_name << "]";
  if (session_id >= 0) out << " [session " << session_id << "]";
  out << " " << msg << std::endl;
}

}  // namespace detail

inline void error(const std::string& msg, int session_id = -1) {
  if (level() >= 0) detail::write("ERROR", session_id, msg);
}
inline void warn(const std::string& msg, int session_id = -1) {
  if (level() >= 1) detail::write("WARN", session_id, msg);
}
inline void info(const std::string& msg, int session_id = -1) {
  if (level() >= 2) detail::write("INFO", session_id, msg);
}
inline void debug(const std::string& msg, int session_id = -1) {
  if (level() >= 3) detail::write("DEBUG", session_id, msg);
}

}  // namespace log
}  // namespace pgpooler
