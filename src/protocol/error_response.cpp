#include "protocol/error_response.hpp"
#include <cstring>
#include <vector>

namespace pgpooler {
namespace protocol {

namespace {

void append_field(std::vector<std::uint8_t>& out, char tag, const std::string& value) {
  out.push_back(static_cast<std::uint8_t>(tag));
  for (char c : value) out.push_back(static_cast<std::uint8_t>(c));
  out.push_back(0);
}

}  // namespace

std::vector<std::uint8_t> build_error_response(
    const std::string& sqlstate, const std::string& message) {
  std::vector<std::uint8_t> body;
  append_field(body, 'S', "FATAL");
  append_field(body, 'C', sqlstate);
  append_field(body, 'M', message);
  body.push_back(0);

  std::uint32_t len = static_cast<std::uint32_t>(body.size());
  std::vector<std::uint8_t> out;
  out.reserve(1 + 4 + body.size());
  out.push_back(0x45);  // 'E'
  out.push_back(static_cast<std::uint8_t>((len >> 24) & 0xff));
  out.push_back(static_cast<std::uint8_t>((len >> 16) & 0xff));
  out.push_back(static_cast<std::uint8_t>((len >> 8) & 0xff));
  out.push_back(static_cast<std::uint8_t>(len & 0xff));
  out.insert(out.end(), body.begin(), body.end());
  return out;
}

}  // namespace protocol
}  // namespace pgpooler
