#include "pool/backend_connection_pool.hpp"
#include <event2/event.h>
#include <event2/bufferevent.h>

namespace pgpooler {
namespace pool {

static bool is_expired(const IdleConnection& c, std::chrono::steady_clock::time_point now,
                       unsigned idle_timeout_sec, unsigned lifetime_sec) {
  if (idle_timeout_sec > 0) {
    auto idle_sec = std::chrono::duration_cast<std::chrono::seconds>(now - c.idle_since).count();
    if (idle_sec >= static_cast<long long>(idle_timeout_sec)) return true;
  }
  if (lifetime_sec > 0) {
    auto age_sec = std::chrono::duration_cast<std::chrono::seconds>(now - c.created_at).count();
    if (age_sec >= static_cast<long long>(lifetime_sec)) return true;
  }
  return false;
}

std::optional<IdleConnection> BackendConnectionPool::take(const std::string& backend_name,
                                                          const std::string& user,
                                                          const std::string& database,
                                                          std::chrono::steady_clock::time_point now,
                                                          unsigned idle_timeout_sec,
                                                          unsigned lifetime_sec) {
  std::lock_guard<std::mutex> lock(mutex_);
  Key key{backend_name, user, database};
  auto it = idle_.find(key);
  if (it == idle_.end() || it->second.empty()) return std::nullopt;
  for (size_t i = 0; i < it->second.size(); ++i) {
    IdleConnection& c = it->second[i];
    if (idle_timeout_sec == 0 && lifetime_sec == 0) {
      IdleConnection out = std::move(c);
      it->second.erase(it->second.begin() + static_cast<std::ptrdiff_t>(i));
      if (it->second.empty()) idle_.erase(it);
      return out;
    }
    if (!is_expired(c, now, idle_timeout_sec, lifetime_sec)) {
      IdleConnection out = std::move(c);
      it->second.erase(it->second.begin() + static_cast<std::ptrdiff_t>(i));
      if (it->second.empty()) idle_.erase(it);
      return out;
    }
  }
  return std::nullopt;
}

void BackendConnectionPool::put(const std::string& backend_name,
                                const std::string& user,
                                const std::string& database,
                                struct bufferevent* bev,
                                std::vector<std::uint8_t> cached_startup_response,
                                std::chrono::steady_clock::time_point created_at) {
  if (!bev) return;
  bufferevent_setcb(bev, nullptr, nullptr, nullptr, nullptr);
  bufferevent_disable(bev, EV_READ);
  std::lock_guard<std::mutex> lock(mutex_);
  Key key{backend_name, user, database};
  IdleConnection c{bev, std::move(cached_startup_response), std::chrono::steady_clock::now(), created_at};
  idle_[key].push_back(std::move(c));
}

std::optional<IdleConnection> BackendConnectionPool::take_one_to_close(
    const std::string& backend_name,
    const std::string& user,
    const std::string& database) {
  std::lock_guard<std::mutex> lock(mutex_);
  Key key{backend_name, user, database};
  auto it = idle_.find(key);
  if (it == idle_.end() || it->second.empty()) return std::nullopt;
  IdleConnection c = std::move(it->second.back());
  it->second.pop_back();
  if (it->second.empty()) idle_.erase(it);
  return c;
}

std::optional<IdleConnection> BackendConnectionPool::take_one_expired(
    const std::string& backend_name,
    const std::string& user,
    const std::string& database,
    std::chrono::steady_clock::time_point now,
    unsigned idle_timeout_sec,
    unsigned lifetime_sec) {
  if (idle_timeout_sec == 0 && lifetime_sec == 0) return std::nullopt;
  std::lock_guard<std::mutex> lock(mutex_);
  Key key{backend_name, user, database};
  auto it = idle_.find(key);
  if (it == idle_.end() || it->second.empty()) return std::nullopt;
  for (size_t i = 0; i < it->second.size(); ++i) {
    if (is_expired(it->second[i], now, idle_timeout_sec, lifetime_sec)) {
      IdleConnection c = std::move(it->second[i]);
      it->second.erase(it->second.begin() + static_cast<std::ptrdiff_t>(i));
      if (it->second.empty()) idle_.erase(it);
      return c;
    }
  }
  return std::nullopt;
}

}  // namespace pool
}  // namespace pgpooler
