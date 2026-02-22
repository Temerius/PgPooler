#include "common/log.hpp"
#include "config/config.hpp"
#include <cerrno>
#include <cstring>
#include <ctime>
#include <fstream>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <sstream>

namespace pgpooler {
namespace log {

namespace {

std::ostream* s_log_out = &std::cerr;
std::ofstream s_log_file;
std::string s_log_path;
bool s_log_append = true;
int s_rotation_size_mb = 0;
std::time_t s_rotation_age_start = 0;
int s_rotation_age_seconds = 0;

/** Expand strftime-style pattern in filename (e.g. pgpooler-%Y-%m-%d.log). */
std::string expand_filename_pattern(const std::string& pattern) {
  std::time_t t = std::time(nullptr);
  std::tm* tm = std::localtime(&t);
  if (!tm) return pattern;
  char buf[256];
  if (std::strftime(buf, sizeof(buf), pattern.c_str(), tm) == 0) return pattern;
  return std::string(buf);
}

}  // namespace

namespace detail {

std::ostream& log_stream() {
  return *s_log_out;
}

}  // namespace detail

void init(const config::LoggingConfig& cfg) {
  set_level(cfg.level);

  std::string path = cfg.file_path;
  if (path.empty() && !cfg.file_directory.empty() && !cfg.file_filename.empty()) {
    std::string name = expand_filename_pattern(cfg.file_filename);
    path = cfg.file_directory;
    if (path.back() != '/' && path.back() != '\\') path += '/';
    path += name;
  }

  if (path.empty()) {
    std::cerr << "PgPooler: logging: file path or directory+filename required" << std::endl;
    return;
  }

  s_log_path = path;
  s_log_append = cfg.file_append;
  s_rotation_size_mb = cfg.rotation_size_mb;
  s_rotation_age_seconds = cfg.rotation_age_seconds;
  s_rotation_age_start = std::time(nullptr);

  set_log_file(path, cfg.file_append);
}

void set_log_file(const std::string& path, bool append) {
  if (s_log_file.is_open()) {
    s_log_file.close();
  }
  if (path.empty()) {
    s_log_out = &std::cerr;
    return;
  }
  std::error_code ec;
  std::filesystem::path p(path);
  std::filesystem::create_directories(p.parent_path(), ec);

  std::ios_base::openmode mode = std::ios_base::out;
  if (append) mode |= std::ios_base::app;
  else mode |= std::ios_base::trunc;

  s_log_file.open(path, mode);
  if (!s_log_file.is_open()) {
    std::cerr << "PgPooler: cannot open log file '" << path << "': " << std::strerror(errno)
              << " (logging to stderr)" << std::endl;
    s_log_out = &std::cerr;
  } else {
    s_log_out = &s_log_file;
  }
  s_log_append = append;
}

}  // namespace log
}  // namespace pgpooler
