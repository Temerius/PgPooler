#include "config/config.hpp"
#include <yaml-cpp/yaml.h>
#include <iostream>
#include <regex>
#include <string>

namespace pgpooler {
namespace config {

namespace {

std::uint16_t parse_port(int v, std::uint16_t default_val) {
  if (v >= 1 && v <= 65535) return static_cast<std::uint16_t>(v);
  return default_val;
}

/** Parse a field (database or user) from a YAML node: scalar -> Exact/Prefix/Regex, sequence -> List. */
bool parse_field_matcher(const YAML::Node& node, FieldMatcher& out) {
  if (!node) return false;
  if (node.IsSequence()) {
    out.type = MatchType::List;
    for (const auto& item : node) {
      if (item.IsScalar()) out.list.push_back(item.Scalar());
    }
    return !out.list.empty();
  }
  if (!node.IsScalar()) return false;
  std::string s = node.Scalar();
  if (s.size() >= 2 && s[0] == '~' && s[1] == ' ') {
    out.type = MatchType::Regex;
    out.value = s.substr(2);
    try {
      out.re = std::regex(out.value);
    } catch (const std::regex_error&) {
      return false;
    }
    return true;
  }
  if (s.size() >= 1 && s.back() == '*' && s.find('*') == s.size() - 1) {
    out.type = MatchType::Prefix;
    out.value = s.substr(0, s.size() - 1);
    return true;
  }
  out.type = MatchType::Exact;
  out.value = std::move(s);
  return true;
}

}  // namespace

bool load_app_config(const std::string& path, AppConfig& out) {
  YAML::Node root;
  try {
    root = YAML::LoadFile(path);
  } catch (const YAML::Exception& e) {
    std::cerr << "PgPooler: failed to load app config " << path << ": " << e.what() << std::endl;
    return false;
  }
  if (!root.IsMap()) {
    std::cerr << "PgPooler: app config root is not a map: " << path << std::endl;
    return false;
  }

  auto listen = root["listen"];
  if (listen && listen.IsMap()) {
    if (listen["host"] && listen["host"].IsScalar()) out.listen_host = listen["host"].Scalar();
    if (listen["port"]) out.listen_port = parse_port(listen["port"].as<int>(6432), 6432);
  }

  auto logging = root["logging"];
  if (logging && logging.IsMap() && logging["path"] && logging["path"].IsScalar()) {
    out.logging_config_path = logging["path"].Scalar();
  }
  auto backends = root["backends"];
  if (backends && backends.IsMap() && backends["path"] && backends["path"].IsScalar()) {
    out.backends_config_path = backends["path"].Scalar();
  }
  auto routing = root["routing"];
  if (routing && routing.IsMap() && routing["path"] && routing["path"].IsScalar()) {
    out.routing_config_path = routing["path"].Scalar();
  }

  if (out.logging_config_path.empty()) {
    std::cerr << "PgPooler: app config must have logging.path: " << path << std::endl;
    return false;
  }
  if (out.backends_config_path.empty()) {
    std::cerr << "PgPooler: app config must have backends.path: " << path << std::endl;
    return false;
  }
  if (out.routing_config_path.empty()) {
    std::cerr << "PgPooler: app config must have routing.path: " << path << std::endl;
    return false;
  }

  return true;
}

bool load_logging_config(const std::string& path, LoggingConfig& out) {
  YAML::Node root;
  try {
    root = YAML::LoadFile(path);
  } catch (const YAML::Exception& e) {
    std::cerr << "PgPooler: failed to load logging config " << path << ": " << e.what() << std::endl;
    return false;
  }
  if (!root.IsMap()) return false;

  if (root["level"] && root["level"].IsScalar()) out.level = root["level"].Scalar();
  if (root["destination"] && root["destination"].IsScalar()) out.destination = root["destination"].Scalar();
  if (root["format"] && root["format"].IsScalar()) out.format = root["format"].Scalar();

  auto file = root["file"];
  if (file && file.IsMap()) {
    if (file["path"] && file["path"].IsScalar()) out.file_path = file["path"].Scalar();
    if (file["directory"] && file["directory"].IsScalar()) out.file_directory = file["directory"].Scalar();
    if (file["filename"] && file["filename"].IsScalar()) out.file_filename = file["filename"].Scalar();
    if (file["append"]) {
      try { out.file_append = file["append"].as<bool>(); } catch (...) {}
    }
    if (file["rotation_age"]) {
      try { out.rotation_age_seconds = file["rotation_age"].as<int>(); } catch (...) {}
    }
    if (file["rotation_size_mb"]) {
      try { out.rotation_size_mb = file["rotation_size_mb"].as<int>(); } catch (...) {}
    }
  }

  return true;
}

bool load_backends_config(const std::string& path, BackendsConfig& out) {
  YAML::Node root;
  try {
    root = YAML::LoadFile(path);
  } catch (const YAML::Exception& e) {
    std::cerr << "PgPooler: failed to load backends config " << path << ": " << e.what() << std::endl;
    return false;
  }
  if (!root.IsMap()) return false;

  auto backends = root["backends"];
  if (!backends || !backends.IsSequence()) {
    std::cerr << "PgPooler: backends config: backends is missing or not a sequence: " << path << std::endl;
    return false;
  }

  out.backends.clear();
  for (const auto& be : backends) {
    if (!be.IsMap()) continue;
    BackendEntry e;
    if (be["name"] && be["name"].IsScalar()) e.name = be["name"].Scalar();
    if (be["host"] && be["host"].IsScalar()) e.host = be["host"].Scalar();
    e.port = parse_port(be["port"] ? be["port"].as<int>(5432) : 5432, 5432);
    if (be["pool_size"]) {
      int ps = be["pool_size"].as<int>(0);
      e.pool_size = (ps > 0) ? static_cast<unsigned>(ps) : 0u;
    }
    if (!e.host.empty()) out.backends.push_back(std::move(e));
  }
  if (out.backends.empty()) {
    std::cerr << "PgPooler: backends config: no backends defined: " << path << std::endl;
    return false;
  }
  return true;
}

bool load_routing_config(const std::string& path, RoutingConfig& out) {
  YAML::Node root;
  try {
    root = YAML::LoadFile(path);
  } catch (const YAML::Exception& e) {
    std::cerr << "PgPooler: failed to load routing config " << path << ": " << e.what() << std::endl;
    return false;
  }
  if (!root.IsMap()) {
    std::cerr << "PgPooler: routing config root is not a map: " << path << std::endl;
    return false;
  }

  auto defaults = root["defaults"];
  if (defaults && defaults.IsMap() && defaults["pool_size"]) {
    int v = defaults["pool_size"].as<int>(0);
    out.defaults.pool_size = (v > 0) ? static_cast<unsigned>(v) : 0u;
  }

  out.routing.clear();
  auto routing = root["routing"];
  if (routing && routing.IsSequence()) {
    for (const auto& rule_node : routing) {
      if (!rule_node.IsMap()) continue;
      RoutingRule rule;
      if (rule_node["default"] && rule_node["default"].IsScalar()) {
        try {
          rule.is_default = rule_node["default"].as<bool>();
        } catch (...) {
          rule.is_default = false;
        }
      }
      if (rule_node["backend"] && rule_node["backend"].IsScalar()) {
        rule.backend_name = rule_node["backend"].Scalar();
      }
      if (rule_node["pool_size"]) {
        int ps = rule_node["pool_size"].as<int>(0);
        rule.pool_size_override = (ps > 0) ? static_cast<unsigned>(ps) : 0u;
      }
      if (rule_node["database"]) {
        FieldMatcher dm;
        if (parse_field_matcher(rule_node["database"], dm)) rule.database = std::move(dm);
      }
      if (rule_node["user"]) {
        FieldMatcher um;
        if (parse_field_matcher(rule_node["user"], um)) rule.user = std::move(um);
      }
      if (rule.is_default || !rule.backend_name.empty()) {
        out.routing.push_back(std::move(rule));
      }
    }
  }

  return true;
}

}  // namespace config
}  // namespace pgpooler
