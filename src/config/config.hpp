#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace pgpooler {
namespace config {

struct BackendEntry {
  std::string name;
  std::string host;
  std::uint16_t port = 5432;
};

/** Log level: "error", "warn", "info", "debug". Default "info". */
struct Config {
  std::string listen_host = "0.0.0.0";
  std::uint16_t listen_port = 6432;
  std::vector<BackendEntry> backends;
  std::string log_level = "info";
};

/** Load config from XML file. Returns nullopt on error (and logs to stderr). */
bool load(const std::string& path, Config& out);

}  // namespace config
}  // namespace pgpooler
