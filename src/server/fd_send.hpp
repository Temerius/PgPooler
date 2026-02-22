#pragma once

#include <cstdint>
#include <optional>
#include <utility>
#include <vector>

namespace pgpooler {
namespace server {

/** Send a client fd and a data payload to the worker over a Unix stream socket (SCM_RIGHTS). Returns true on success. */
bool send_fd_and_payload(int socket_fd, int client_fd, const std::vector<std::uint8_t>& payload);

/** Non-blocking receive state for worker (one message may be split across reads). */
struct WorkerRecvState {
  int pending_fd = -1;
  std::uint32_t payload_len = 0;
  std::vector<std::uint8_t> payload;
};

/** Try to receive one fd+payload. Returns the pair when a full message is received; nullopt if need more data or error. Updates state for partial receives. */
std::optional<std::pair<int, std::vector<std::uint8_t>>> try_recv_fd_and_payload(int socket_fd, WorkerRecvState& state);

}  // namespace server
}  // namespace pgpooler
