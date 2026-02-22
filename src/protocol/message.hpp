#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

struct evbuffer;

namespace pgpooler {
namespace protocol {

// PostgreSQL wire protocol uses network byte order (big-endian) for Int32.

/** Returns byte length of the first client packet (SSL request + optional Startup, or just Startup), or 0 if incomplete. Does not drain input. */
size_t first_client_packet_length(struct evbuffer* input);

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

/** ReadyForQuery has type 'Z' (0x5A). Body: Int32 length (4), then 1 byte transaction state. */
constexpr unsigned char MSG_READY_FOR_QUERY = 'Z';
/** Transaction state in ReadyForQuery: 'I' idle, 'T' in transaction, 'E' in failed transaction. */
constexpr unsigned char TXSTATE_IDLE = 'I';
constexpr unsigned char TXSTATE_BLOCK = 'T';
constexpr unsigned char TXSTATE_FAILED = 'E';

/** If msg is ReadyForQuery (type 'Z', length 5), returns the state byte ('I'/'T'/'E'). Otherwise nullopt. */
std::optional<unsigned char> get_ready_for_query_state(const std::vector<std::uint8_t>& msg);

/** Build a simple Query message (type 'Q'): length (4) + query string (null-terminated). */
std::vector<std::uint8_t> build_query_message(const std::string& query);

/** Extract a parameter value from StartupMessage (e.g. "user", "database").
 * StartupMessage body: Int32 length, Int32 version, then key\\0value\\0... ending with \\0.
 * Returns nullopt if key not found or message too short. */
std::optional<std::string> extract_startup_parameter(
    const std::vector<std::uint8_t>& startup_msg, const char* key);

}  // namespace protocol
}  // namespace pgpooler
