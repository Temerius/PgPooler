#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

struct bufferevent;

namespace pgpooler {
namespace pool {

/** One idle backend connection: bev + cached startup response + timestamps for timeout eviction. */
struct IdleConnection {
  struct bufferevent* bev = nullptr;
  std::vector<std::uint8_t> cached_startup_response;
  std::chrono::steady_clock::time_point idle_since{std::chrono::steady_clock::now()};
  std::chrono::steady_clock::time_point created_at{std::chrono::steady_clock::now()};
};

/** Thread-safe pool of idle backend connections keyed by (backend_name, user, database). */
class BackendConnectionPool {
 public:
  BackendConnectionPool() = default;
  ~BackendConnectionPool() = default;

  BackendConnectionPool(const BackendConnectionPool&) = delete;
  BackendConnectionPool& operator=(const BackendConnectionPool&) = delete;

  /** Take an idle connection for (backend_name, user, database). Returns nullopt if none.
   * If idle_timeout_sec/lifetime_sec > 0, skips expired entries (does not return them). */
  std::optional<IdleConnection> take(const std::string& backend_name,
                                    const std::string& user,
                                    const std::string& database,
                                    std::chrono::steady_clock::time_point now,
                                    unsigned idle_timeout_sec,
                                    unsigned lifetime_sec);

  /** Return a connection to the pool. Disables read and clears callbacks on bev.
   * created_at is when the connection was first established (for server_lifetime). */
  void put(const std::string& backend_name,
           const std::string& user,
           const std::string& database,
           struct bufferevent* bev,
           std::vector<std::uint8_t> cached_startup_response,
           std::chrono::steady_clock::time_point created_at);

  /** Remove one idle connection (e.g. to close it when session that had put disconnects). */
  std::optional<IdleConnection> take_one_to_close(const std::string& backend_name,
                                                   const std::string& user,
                                                   const std::string& database);

  /** Remove and return one idle connection that is expired (idle or lifetime). Caller must close bev and release slot. */
  std::optional<IdleConnection> take_one_expired(const std::string& backend_name,
                                                  const std::string& user,
                                                  const std::string& database,
                                                  std::chrono::steady_clock::time_point now,
                                                  unsigned idle_timeout_sec,
                                                  unsigned lifetime_sec);

 private:
  std::mutex mutex_;
  struct Key {
    std::string backend_name;
    std::string user;
    std::string database;
    bool operator<(const Key& o) const {
      if (backend_name != o.backend_name) return backend_name < o.backend_name;
      if (user != o.user) return user < o.user;
      return database < o.database;
    }
  };
  std::map<Key, std::vector<IdleConnection>> idle_;
};

}  // namespace pool
}  // namespace pgpooler
