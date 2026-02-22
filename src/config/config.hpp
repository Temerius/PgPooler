#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <regex>
#include <string>
#include <vector>

namespace pgpooler {
namespace config {

struct BackendEntry {
  std::string name;
  std::string host;
  std::uint16_t port = 5432;
  /** Max connections to this backend (0 = unlimited). When exceeded, reject with 53300. */
  unsigned pool_size = 0;
};

/** Result of routing: backend to use and effective pool_size for this connection. */
struct ResolvedBackend {
  std::string name;
  std::string host;
  std::uint16_t port = 5432;
  unsigned pool_size = 0;  // 0 = unlimited
};

/** Resolver: (user, database) -> backend to use. Used when first message is Startup or for SSL default. */
using BackendResolver =
    std::function<std::optional<ResolvedBackend>(const std::string& user, const std::string& database)>;

/** Thread-safe: limits concurrent connections per backend. acquire() before connecting, release() on disconnect. */
class PoolManager {
 public:
  explicit PoolManager(const std::vector<BackendEntry>& backends);
  /** Returns true if a slot was acquired. When false, caller must send ErrorResponse and close. */
  bool acquire(const std::string& backend_name);
  void release(const std::string& backend_name);

 private:
  std::mutex mutex_;
  std::map<std::string, std::pair<unsigned, unsigned>> state_;  // name -> (current, max); max 0 = unlimited
};

/** Match type for database/user in routing rules. */
enum class MatchType { Exact, List, Prefix, Regex };

/** Matcher for one field (database or user). */
struct FieldMatcher {
  MatchType type = MatchType::Exact;
  std::string value;               // for Exact, Prefix, or regex pattern
  std::vector<std::string> list;    // for List
  std::optional<std::regex> re;    // compiled for Regex (set when type == Regex and pattern valid)

  bool match(const std::string& s) const;
};

/** One routing rule: conditions + backend + optional pool_size override. */
struct RoutingRule {
  std::optional<FieldMatcher> database;
  std::optional<FieldMatcher> user;
  bool is_default = false;
  std::string backend_name;
  unsigned pool_size_override = 0;  // 0 = use backend/defaults
};

/** Global defaults (pool_size, etc.) for routing config. */
struct Defaults {
  unsigned pool_size = 0;
};

/** Router: first matching rule wins. */
class Router {
 public:
  Router(const std::vector<BackendEntry>& backends,
         const Defaults& defaults,
         const std::vector<RoutingRule>& rules);
  std::optional<ResolvedBackend> resolve(const std::string& user, const std::string& database) const;

 private:
  const std::vector<BackendEntry>& backends_;
  Defaults defaults_;
  std::vector<RoutingRule> rules_;
};

/** Main application config (YAML): listen, paths to logging/backends/routing configs. */
struct AppConfig {
  std::string listen_host = "0.0.0.0";
  std::uint16_t listen_port = 6432;
  std::string logging_config_path;
  std::string backends_config_path;
  std::string routing_config_path;
};

/** Logging config (YAML): level, destination, file options, format, rotation. */
struct LoggingConfig {
  std::string level = "info";
  std::string destination = "file";   // "file" only (no stderr in normal operation)
  std::string file_directory;          // directory for log files (used with file_filename)
  std::string file_filename;          // pattern e.g. "pgpooler-%Y-%m-%d.log" (strftime)
  std::string file_path;              // or single file path (if set, directory/filename ignored)
  bool file_append = true;
  std::string format = "text";        // "text" (timestamp level [session] message)
  int rotation_age_seconds = 0;       // 0 = no time-based rotation
  int rotation_size_mb = 0;           // 0 = no size-based rotation
};

/** Backends config (YAML): list of PostgreSQL backends. */
struct BackendsConfig {
  std::vector<BackendEntry> backends;
};

/** Routing config (YAML): pool defaults and routing rules only (backend names refer to backends config). */
struct RoutingConfig {
  Defaults defaults;
  std::vector<RoutingRule> routing;
};

/** Load main application config from YAML. Returns false on error (logs to stderr). */
bool load_app_config(const std::string& path, AppConfig& out);

/** Load logging config from YAML. Returns false on error. */
bool load_logging_config(const std::string& path, LoggingConfig& out);

/** Load backends config from YAML. Returns false on error. */
bool load_backends_config(const std::string& path, BackendsConfig& out);

/** Load routing config from YAML (defaults + rules only, no backends). Returns false on error. */
bool load_routing_config(const std::string& path, RoutingConfig& out);

/** Build a resolver: backends + routing config; first backend if no rules, else Router. */
BackendResolver make_resolver(const std::vector<BackendEntry>& backends,
                              const RoutingConfig& routing_cfg,
                              const Router* router);

}  // namespace config
}  // namespace pgpooler
