#include "protocol/message.hpp"
#include <event2/buffer.h>
#include <algorithm>
#include <cstring>
#include <string>

namespace pgpooler {
namespace protocol {

namespace {

std::uint32_t read_be32(const unsigned char* p) {
  return (static_cast<std::uint32_t>(p[0]) << 24) |
         (static_cast<std::uint32_t>(p[1]) << 16) |
         (static_cast<std::uint32_t>(p[2]) << 8) |
         static_cast<std::uint32_t>(p[3]);
}

}  // namespace

bool try_extract_length_prefixed_message(struct evbuffer* input, std::vector<std::uint8_t>& out) {
  const size_t avail = evbuffer_get_length(input);
  if (avail < 4) return false;

  unsigned char* p = evbuffer_pullup(input, 4);
  if (!p) return false;

  const std::uint32_t len = read_be32(p);
  if (len < 4 || len > 1024 * 1024) return false;  // sanity
  if (avail < len) return false;

  out.resize(len);
  const size_t removed = evbuffer_remove(input, out.data(), len);
  if (removed != len) return false;
  return true;
}

bool try_extract_typed_message(struct evbuffer* input, std::vector<std::uint8_t>& out) {
  const size_t avail = evbuffer_get_length(input);
  if (avail < 5) return false;

  unsigned char* p = evbuffer_pullup(input, 5);
  if (!p) return false;

  const std::uint32_t len = read_be32(p + 1);
  if (len < 4 || len > 1024 * 1024) return false;
  const size_t total = 1 + len;
  if (avail < total) return false;

  out.resize(total);
  const size_t removed = evbuffer_remove(input, out.data(), total);
  if (removed != total) return false;
  return true;
}

std::optional<std::string> extract_startup_parameter(
    const std::vector<std::uint8_t>& startup_msg, const char* key) {
  const size_t key_len = std::strlen(key);
  if (key_len == 0) return std::nullopt;
  if (startup_msg.size() < 8) return std::nullopt;
  size_t i = 8;
  while (i + key_len + 1 <= startup_msg.size()) {
    if (std::memcmp(&startup_msg[i], key, key_len) == 0 && startup_msg[i + key_len] == '\0') {
      i += key_len + 1;
      if (i >= startup_msg.size()) return std::nullopt;
      auto end_it = std::find(startup_msg.begin() + static_cast<std::ptrdiff_t>(i), startup_msg.end(), '\0');
      if (end_it == startup_msg.end()) return std::nullopt;
      return std::string(reinterpret_cast<const char*>(&startup_msg[i]),
                         static_cast<size_t>(end_it - (startup_msg.begin() + static_cast<std::ptrdiff_t>(i))));
    }
    while (i < startup_msg.size() && startup_msg[i] != '\0') ++i;
    if (i >= startup_msg.size()) break;
    ++i;
    while (i < startup_msg.size() && startup_msg[i] != '\0') ++i;
    if (i >= startup_msg.size()) break;
    ++i;
  }
  return std::nullopt;
}

}  // namespace protocol
}  // namespace pgpooler
