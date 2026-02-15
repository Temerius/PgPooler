#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

struct evbuffer;

namespace pgpooler {
namespace protocol {

// PostgreSQL wire protocol uses network byte order (big-endian) for Int32.

/** First message from client: StartupMessage, SSLRequest, or GSSENCRequest.
 * Format: Int32 length (including self), then length-4 bytes. No type byte. */
bool try_extract_length_prefixed_message(struct evbuffer* input, std::vector<std::uint8_t>& out);

/** Subsequent messages: Byte1 type, Int32 length (length of message contents including
 * the 4-byte length field itself; total message size = 1 + len).
 * Returns true and fills out if a complete message is available. */
bool try_extract_typed_message(struct evbuffer* input, std::vector<std::uint8_t>& out);

/** Returns the message type byte for a typed message, or 0 if size < 1. */
inline unsigned char get_message_type(const std::vector<std::uint8_t>& msg) {
  return msg.empty() ? 0 : msg[0];
}

/** ReadyForQuery has type 'Z' (0x5A). */
constexpr unsigned char MSG_READY_FOR_QUERY = 'Z';

}  // namespace protocol
}  // namespace pgpooler
