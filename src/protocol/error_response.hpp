#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace pgpooler {
namespace protocol {

/** Build a PostgreSQL ErrorResponse message (type 'E') for sending to client.
 * Format: Byte1('E'), Int32(length), then fields: Byte1(tag) + string\\0 ...
 * End with Byte1(0). SQLSTATE must be 5 chars (e.g. "53300"). */
std::vector<std::uint8_t> build_error_response(
    const std::string& sqlstate,   // e.g. "53300"
    const std::string& message);  // human-readable, e.g. "sorry, too many clients already"

}  // namespace protocol
}  // namespace pgpooler
