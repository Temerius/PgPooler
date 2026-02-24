#include "protocol/message.hpp"
#include <event2/buffer.h>
#include <algorithm>
#include <cctype>
#include <cstdlib>
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

namespace {

constexpr std::uint32_t SSL_REQUEST_CODE = 80877103;

}  // namespace

size_t first_client_packet_length(struct evbuffer* input) {
  const size_t avail = evbuffer_get_length(input);
  if (avail < 4) return 0;
  unsigned char* p = evbuffer_pullup(input, 4);
  if (!p) return 0;
  std::uint32_t len = read_be32(p);
  if (len < 4 || len > 1024 * 1024) return 0;
  if (avail < len) return 0;  // length field includes itself
  if (len == 8 && avail >= 8) {
    unsigned char* q = evbuffer_pullup(input, 8);
    if (!q) return 0;
    std::uint32_t code = read_be32(q + 4);
    if (code == SSL_REQUEST_CODE) {
      if (avail < 8 + 4) return 0;
      q = evbuffer_pullup(input, 8 + 4);
      if (!q) return 0;
      std::uint32_t len2 = read_be32(q + 8);
      if (len2 < 4 || len2 > 1024 * 1024) return 0;
      if (avail < 8u + 4u + len2) return 0;
      return 8 + 4 + len2;
    }
  }
  return len;
}

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

std::optional<unsigned char> get_ready_for_query_state(const std::vector<std::uint8_t>& msg) {
  if (msg.size() < 6 || msg[0] != MSG_READY_FOR_QUERY) return std::nullopt;
  const std::uint32_t len = read_be32(&msg[1]);
  if (len != 5) return std::nullopt;  // 4 (length) + 1 (state)
  unsigned char state = msg[5];
  if (state != TXSTATE_IDLE && state != TXSTATE_BLOCK && state != TXSTATE_FAILED) return std::nullopt;
  return state;
}

std::vector<std::uint8_t> build_query_message(const std::string& query) {
  const std::uint32_t len = 4 + static_cast<std::uint32_t>(query.size()) + 1;  // length field + string + nul
  std::vector<std::uint8_t> out;
  out.reserve(1 + 4 + query.size() + 1);
  out.push_back('Q');
  out.push_back(static_cast<std::uint8_t>((len >> 24) & 0xff));
  out.push_back(static_cast<std::uint8_t>((len >> 16) & 0xff));
  out.push_back(static_cast<std::uint8_t>((len >> 8) & 0xff));
  out.push_back(static_cast<std::uint8_t>(len & 0xff));
  out.insert(out.end(), query.begin(), query.end());
  out.push_back('\0');
  return out;
}

std::vector<std::uint8_t> trim_startup_response_to_post_auth(const std::vector<std::uint8_t>& full) {
  const unsigned char* p = full.data();
  const size_t total = full.size();
  size_t offset = 0;
  while (offset + 5 <= total) {
    unsigned char type = p[offset];
    std::uint32_t len = read_be32(p + offset + 1);
    if (len < 4 || len > 1024 * 1024) break;
    size_t msg_size = 1 + len;
    if (offset + msg_size > total) break;
    if (type == 'R' && len == 8 && offset + 9 <= total && read_be32(p + offset + 5) == 0) {
      return std::vector<std::uint8_t>(full.begin() + static_cast<std::ptrdiff_t>(offset), full.end());
    }
    offset += msg_size;
  }
  return full;
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

std::optional<CommandCompleteTag> parse_command_complete(const std::vector<std::uint8_t>& msg) {
  if (msg.size() < 6 || msg[0] != MSG_COMMAND_COMPLETE) return std::nullopt;
  const std::uint32_t len = read_be32(&msg[1]);
  if (len < 4 || msg.size() < 1u + len) return std::nullopt;
  const char* tag_start = reinterpret_cast<const char*>(&msg[5]);
  const size_t tag_len = len - 4;
  if (tag_len == 0) return std::nullopt;
  std::string tag(tag_start, tag_len);
  while (!tag.empty() && tag.back() == '\0') tag.pop_back();
  CommandCompleteTag out;
  size_t i = 0;
  while (i < tag.size() && (std::isalpha(static_cast<unsigned char>(tag[i])) || tag[i] == '_')) ++i;
  out.command_type = tag.substr(0, i);
  if (out.command_type.empty()) return std::nullopt;
  while (i < tag.size() && (tag[i] == ' ' || tag[i] == '\t')) ++i;
  if (i >= tag.size()) return out;
  const std::string rest = tag.substr(i);
  long first_num = -1;
  long last_num = -1;
  const char* p = rest.c_str();
  char* end = nullptr;
  for (;;) {
    while (*p == ' ' || *p == '\t') ++p;
    if (!*p) break;
    long n = std::strtol(p, &end, 10);
    if (end == p) break;
    if (first_num < 0) first_num = n;
    last_num = n;
    p = end;
  }
  if (out.command_type == "SELECT" && first_num >= 0)
    out.rows_returned = static_cast<std::int64_t>(first_num);
  if (last_num >= 0)
    out.rows_affected = static_cast<std::int64_t>(last_num);
  return out;
}

}  // namespace protocol
}  // namespace pgpooler
