#include "config/config.hpp"
#include <algorithm>
#include <iostream>
#include <string>
#include <tuple>

namespace pgpooler {
namespace config {

bool FieldMatcher::match(const std::string& s) const {
  switch (type) {
    case MatchType::Exact:
      return s == value;
    case MatchType::List:
      return std::find(list.begin(), list.end(), s) != list.end();
    case MatchType::Prefix:
      return value.size() <= s.size() && s.compare(0, value.size(), value) == 0;
    case MatchType::Regex:
      return re.has_value() && std::regex_match(s, *re);
    default:
      return false;
  }
}

Router::Router(const std::vector<BackendEntry>& backends,
               const Defaults& defaults,
               const std::vector<RoutingRule>& rules)
    : backends_(backends), defaults_(defaults), rules_(rules) {}

std::optional<ResolvedBackend> Router::resolve(const std::string& user, const std::string& database) const {
  for (const auto& rule : rules_) {
    if (rule.is_default) {
      // Default rule: match any
    } else {
      if (rule.database.has_value() && !rule.database->match(database)) continue;
      if (rule.user.has_value() && !rule.user->match(user)) continue;
    }
    // Find backend by name
    const BackendEntry* be = nullptr;
    for (const auto& b : backends_) {
      if (b.name == rule.backend_name) {
        be = &b;
        break;
      }
    }
    if (!be) continue;
    ResolvedBackend out;
    out.name = be->name;
    out.host = be->host;
    out.port = be->port;
    out.pool_size = (rule.pool_size_override != 0) ? rule.pool_size_override : be->pool_size;
    if (out.pool_size == 0) out.pool_size = defaults_.pool_size;
    out.pool_mode = rule.has_pool_mode_override ? rule.pool_mode_override : be->pool_mode;
    out.server_idle_timeout_sec = be->server_idle_timeout_sec;
    out.server_lifetime_sec = be->server_lifetime_sec;
    out.query_wait_timeout_sec = be->query_wait_timeout_sec;
    return out;
  }
  return std::nullopt;
}

BackendResolver make_resolver(const std::vector<BackendEntry>& backends,
                              const RoutingConfig& routing_cfg,
                              const Router* router) {
  if (!router || routing_cfg.routing.empty()) {
    if (backends.empty()) return [](const std::string&, const std::string&) { return std::nullopt; };
    const BackendEntry& b = backends.front();
    ResolvedBackend fixed;
    fixed.name = b.name;
    fixed.host = b.host;
    fixed.port = b.port;
    fixed.pool_size = b.pool_size;
    fixed.pool_mode = b.pool_mode;
    fixed.server_idle_timeout_sec = b.server_idle_timeout_sec;
    fixed.server_lifetime_sec = b.server_lifetime_sec;
    fixed.query_wait_timeout_sec = b.query_wait_timeout_sec;
    return [fixed](const std::string&, const std::string&) { return fixed; };
  }
  const Router* r = router;
  return [r](const std::string& user, const std::string& database) {
    return r->resolve(user, database);
  };
}

PoolManager::PoolManager(const std::vector<BackendEntry>& backends) {
  for (const auto& b : backends) {
    state_[b.name] = std::make_tuple(0u, 0u, b.pool_size);
  }
}

bool PoolManager::acquire(const std::string& backend_name) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = state_.find(backend_name);
  if (it == state_.end()) return false;
  unsigned& in_use = std::get<0>(it->second);
  unsigned& in_pool = std::get<1>(it->second);
  unsigned max_val = std::get<2>(it->second);
  if (max_val != 0 && in_use + in_pool >= max_val) return false;
  ++in_use;
  return true;
}

void PoolManager::release(const std::string& backend_name) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = state_.find(backend_name);
  if (it != state_.end() && std::get<0>(it->second) > 0) --std::get<0>(it->second);
}

void PoolManager::put_backend(const std::string& backend_name) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = state_.find(backend_name);
  if (it == state_.end()) return;
  unsigned& in_use = std::get<0>(it->second);
  unsigned& in_pool = std::get<1>(it->second);
  if (in_use > 0) {
    --in_use;
    ++in_pool;
  }
}

bool PoolManager::take_backend(const std::string& backend_name) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = state_.find(backend_name);
  if (it == state_.end()) return false;
  unsigned& in_pool = std::get<1>(it->second);
  if (in_pool == 0) return false;
  --in_pool;
  ++std::get<0>(it->second);
  return true;
}

}  // namespace config
}  // namespace pgpooler
