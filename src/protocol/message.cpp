#include "protocol/message.hpp"
#include <event2/buffer.h>

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

}  // namespace protocol
}  // namespace pgpooler
