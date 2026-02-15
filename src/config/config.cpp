#include "config/config.hpp"
#include <pugixml.hpp>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <string>

namespace pgpooler {
namespace config {

namespace {

std::uint16_t parse_port(const char* s, std::uint16_t default_val) {
  if (!s || !*s) return default_val;
  try {
    int v = std::stoi(s);
    if (v >= 1 && v <= 65535) return static_cast<std::uint16_t>(v);
  } catch (...) {}
  return default_val;
}

}  // namespace

bool load(const std::string& path, Config& out) {
  pugi::xml_document doc;
  pugi::xml_parse_result result = doc.load_file(path.c_str());
  if (!result) {
    std::cerr << "PgPooler config: failed to load " << path << ": " << result.description() << std::endl;
    return false;
  }

  auto root = doc.child("pgpooler");
  if (!root) {
    std::cerr << "PgPooler config: root element <pgpooler> not found" << std::endl;
    return false;
  }

  auto listen = root.child("listen");
  if (listen) {
    const char* h = listen.attribute("host").as_string();
    if (h && h[0]) out.listen_host = h;
    out.listen_port = parse_port(listen.attribute("port").as_string(), out.listen_port);
  }

  out.backends.clear();
  for (auto be : root.child("backends").children("backend")) {
    BackendEntry e;
    e.name = be.attribute("name").as_string("");
    e.host = be.attribute("host").as_string("");
    e.port = parse_port(be.attribute("port").as_string(), 5432);
    if (!e.host.empty()) out.backends.push_back(std::move(e));
  }

  if (out.backends.empty()) {
    std::cerr << "PgPooler config: no backends defined" << std::endl;
    return false;
  }

  auto log_el = root.child("log");
  if (log_el) {
    const char* level = log_el.attribute("level").as_string("");
    if (level && level[0]) out.log_level = level;
  }

  return true;
}

}  // namespace config
}  // namespace pgpooler
