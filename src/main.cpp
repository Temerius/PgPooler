#include "common/log.hpp"
#include "config/config.hpp"
#include "server/listener.hpp"
#include <event2/event.h>
#include <csignal>
#include <cstdlib>
#include <string>

namespace {

std::string getenv_default(const char* name, const std::string& default_value) {
  const char* v = std::getenv(name);
  return v && v[0] != '\0' ? std::string(v) : default_value;
}

}  // namespace

int main(int argc, char* argv[]) {
  (void)argc;
  (void)argv;

  std::signal(SIGPIPE, SIG_IGN);

  std::string config_path = getenv_default("CONFIG_PATH", "config.xml");
  pgpooler::config::Config cfg;
  if (!pgpooler::config::load(config_path, cfg)) {
    pgpooler::log::error("cannot load config from " + config_path);
    return 1;
  }
  pgpooler::log::set_level(cfg.log_level);
  pgpooler::log::debug("log level=" + cfg.log_level);

  const auto& backend = cfg.backends.front();
  pgpooler::log::info("config " + config_path + " -> listen " + cfg.listen_host + ":" +
                      std::to_string(cfg.listen_port) + ", backend " + backend.name + " " +
                      backend.host + ":" + std::to_string(backend.port));

  struct event_base* base = event_base_new();
  if (!base) {
    pgpooler::log::error("event_base_new failed");
    return 1;
  }

  pgpooler::server::BackendConfig backend_cfg{backend.host, backend.port};
  pgpooler::server::Listener listener(base, cfg.listen_host.c_str(), cfg.listen_port, backend_cfg);
  if (!listener.ok()) {
    pgpooler::log::error("failed to bind listener on " + cfg.listen_host + ":" +
                         std::to_string(cfg.listen_port));
    event_base_free(base);
    return 1;
  }

  pgpooler::log::info("ready, listening on " + cfg.listen_host + ":" + std::to_string(cfg.listen_port) +
                      " (connect with psql -h <host> -p " + std::to_string(cfg.listen_port) + " -U <user> -d <db>)");

  event_base_dispatch(base);
  event_base_free(base);
  return 0;
}
