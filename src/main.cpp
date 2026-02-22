#include "common/log.hpp"
#include "config/config.hpp"
#include "pool/backend_connection_pool.hpp"
#include "pool/connection_wait_queue.hpp"
#include "server/dispatcher.hpp"
#include "server/listener.hpp"
#include <event2/event.h>
#include <csignal>
#include <cstdlib>
#include <string>
#include <unistd.h>
#include <sys/socket.h>
#include <map>
#include <vector>

namespace {

std::string getenv_default(const char* name, const std::string& default_value) {
  const char* v = std::getenv(name);
  return v && v[0] != '\0' ? std::string(v) : default_value;
}

/** If path is relative, resolve relative to the directory of base_file. */
std::string resolve_path(const std::string& base_file, const std::string& path) {
  if (path.empty()) return path;
  if (path[0] == '/' || (path.size() >= 2 && path[1] == ':')) return path;  // absolute
  std::string::size_type pos = base_file.find_last_of("/\\");
  if (pos == std::string::npos) return path;
  return base_file.substr(0, pos + 1) + path;
}

}  // namespace

int main(int argc, char* argv[]) {
  (void)argc;
  (void)argv;

  std::signal(SIGPIPE, SIG_IGN);

  const std::string app_config_path = getenv_default("CONFIG_PATH", "pgpooler.yaml");

  pgpooler::config::AppConfig app_cfg;
  if (!pgpooler::config::load_app_config(app_config_path, app_cfg)) {
    std::cerr << "PgPooler: cannot load app config from " << app_config_path << std::endl;
    return 1;
  }

  const std::string logging_path = resolve_path(app_config_path, app_cfg.logging_config_path);
  pgpooler::config::LoggingConfig logging_cfg;
  if (!pgpooler::config::load_logging_config(logging_path, logging_cfg)) {
    std::cerr << "PgPooler: cannot load logging config from " << logging_path << std::endl;
    return 1;
  }
  pgpooler::log::init(logging_cfg);

  const std::string backends_path = resolve_path(app_config_path, app_cfg.backends_config_path);
  pgpooler::config::BackendsConfig backends_cfg;
  if (!pgpooler::config::load_backends_config(backends_path, backends_cfg)) {
    pgpooler::log::error("cannot load backends config from " + backends_path);
    return 1;
  }

  const std::string routing_path = resolve_path(app_config_path, app_cfg.routing_config_path);
  pgpooler::config::RoutingConfig routing_cfg;
  if (!pgpooler::config::load_routing_config(routing_path, routing_cfg)) {
    pgpooler::log::error("cannot load routing config from " + routing_path);
    return 1;
  }

  const auto& backends = backends_cfg.backends;
  pgpooler::config::Router* router_ptr = nullptr;
  pgpooler::config::Router router(backends, routing_cfg.defaults, routing_cfg.routing);
  if (!routing_cfg.routing.empty()) {
    router_ptr = &router;
    pgpooler::log::info("app config " + app_config_path + " -> listen " + app_cfg.listen_host + ":" +
                        std::to_string(app_cfg.listen_port) + ", backends " + backends_path +
                        " (" + std::to_string(backends.size()) + "), routing " + routing_path +
                        " (" + std::to_string(routing_cfg.routing.size()) + " rules)");
  } else {
    pgpooler::log::info("app config " + app_config_path + " -> listen " + app_cfg.listen_host + ":" +
                        std::to_string(app_cfg.listen_port) + ", backends " + backends_path +
                        " -> " + backends.front().name);
  }

  pgpooler::config::BackendResolver resolver =
      pgpooler::config::make_resolver(backends, routing_cfg, router_ptr);

  struct event_base* base = event_base_new();
  if (!base) {
    pgpooler::log::error("event_base_new failed");
    return 1;
  }

  if (!app_cfg.workers.empty()) {
    std::map<std::string, std::size_t> backend_to_worker;
    for (std::size_t i = 0; i < app_cfg.workers.size(); ++i) {
      for (const auto& name : app_cfg.workers[i].backends)
        backend_to_worker[name] = i;
    }
    std::vector<std::pair<int, int>> pairs(app_cfg.workers.size());
    for (std::size_t i = 0; i < app_cfg.workers.size(); ++i) {
      int fds[2];
      if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) {
        pgpooler::log::error("socketpair failed for worker " + std::to_string(i));
        event_base_free(base);
        return 1;
      }
      pairs[i].first = fds[0];
      pairs[i].second = fds[1];
    }
    for (std::size_t i = 0; i < app_cfg.workers.size(); ++i) {
      pid_t pid = fork();
      if (pid < 0) {
        pgpooler::log::error("fork failed");
        event_base_free(base);
        return 1;
      }
      if (pid == 0) {
        for (std::size_t j = 0; j < pairs.size(); ++j) {
          if (j != i) {
            close(pairs[j].first);
            close(pairs[j].second);
          } else {
            close(pairs[j].first);
          }
        }
        pgpooler::server::run_worker(i, pairs[i].second,
            app_cfg.workers[i].backends, app_config_path,
            app_cfg.backends_config_path, app_cfg.routing_config_path);
        _exit(0);
      }
    }
    for (auto& p : pairs)
      close(p.second);
    std::vector<int> worker_fds;
    for (auto& p : pairs)
      worker_fds.push_back(p.first);
    pgpooler::server::run_dispatcher(base, app_cfg.listen_host, app_cfg.listen_port,
        worker_fds, backend_to_worker, resolver);
    event_base_free(base);
    return 0;
  }

  pgpooler::config::PoolManager pool_manager(backends);
  pgpooler::pool::BackendConnectionPool connection_pool;
  pgpooler::pool::ConnectionWaitQueue wait_queue(base);

  pgpooler::server::Listener listener(base, app_cfg.listen_host.c_str(), app_cfg.listen_port,
                                       resolver, &pool_manager, &connection_pool, &wait_queue);
  if (!listener.ok()) {
    pgpooler::log::error("failed to bind listener on " + app_cfg.listen_host + ":" +
                         std::to_string(app_cfg.listen_port));
    event_base_free(base);
    return 1;
  }

  pgpooler::log::info("ready, listening on " + app_cfg.listen_host + ":" + std::to_string(app_cfg.listen_port) +
                      " (connect with psql -h <host> -p " + std::to_string(app_cfg.listen_port) + " -U <user> -d <db>)");

  event_base_dispatch(base);
  event_base_free(base);
  return 0;
}
