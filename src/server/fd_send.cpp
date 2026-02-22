#include "server/fd_send.hpp"
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <stdexcept>

#if defined(__linux__)
#include <linux/un.h>
#else
#include <sys/un.h>
#endif

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif

namespace pgpooler {
namespace server {

bool send_fd_and_payload(int socket_fd, int client_fd, const std::vector<std::uint8_t>& payload) {
  std::uint32_t len = static_cast<std::uint32_t>(payload.size());
  if (len != payload.size()) return false;
  char len_buf[4];
  len_buf[0] = static_cast<char>((len >> 24) & 0xff);
  len_buf[1] = static_cast<char>((len >> 16) & 0xff);
  len_buf[2] = static_cast<char>((len >> 8) & 0xff);
  len_buf[3] = static_cast<char>(len & 0xff);

  struct msghdr msg = {};
  struct iovec iov[2];
  iov[0].iov_base = len_buf;
  iov[0].iov_len = 4;
  iov[1].iov_base = const_cast<std::uint8_t*>(payload.data());
  iov[1].iov_len = payload.size();
  msg.msg_iov = iov;
  msg.msg_iovlen = 2;

  char cmsg_buf[CMSG_SPACE(sizeof(int))];
  msg.msg_control = cmsg_buf;
  msg.msg_controllen = sizeof(cmsg_buf);
  struct cmsghdr* cmsg = CMSG_FIRSTHDR(&msg);
  cmsg->cmsg_level = SOL_SOCKET;
  cmsg->cmsg_type = SCM_RIGHTS;
  cmsg->cmsg_len = CMSG_LEN(sizeof(int));
  *reinterpret_cast<int*>(CMSG_DATA(cmsg)) = client_fd;
  msg.msg_controllen = cmsg->cmsg_len;

  ssize_t n = sendmsg(socket_fd, &msg, MSG_NOSIGNAL);
  if (n < 0 || static_cast<size_t>(n) != 4 + payload.size()) {
    return false;
  }
  return true;
}

std::optional<std::pair<int, std::vector<std::uint8_t>>> try_recv_fd_and_payload(int socket_fd, WorkerRecvState& state) {
  if (state.pending_fd >= 0 && state.payload.size() >= state.payload_len) {
    int fd = state.pending_fd;
    state.pending_fd = -1;
    state.payload_len = 0;
    std::vector<std::uint8_t> payload = std::move(state.payload);
    state.payload.clear();
    return std::make_pair(fd, std::move(payload));
  }

  if (state.pending_fd < 0) {
    std::uint8_t len_buf[4];
    char cmsg_buf[CMSG_SPACE(sizeof(int))];
    struct msghdr msg = {};
    struct iovec iov;
    iov.iov_base = len_buf;
    iov.iov_len = 4;
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    msg.msg_control = cmsg_buf;
    msg.msg_controllen = sizeof(cmsg_buf);

    ssize_t n = recvmsg(socket_fd, &msg, MSG_DONTWAIT);
    if (n < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) return std::nullopt;
      return std::nullopt;
    }
    if (n == 0) return std::nullopt;
    if (n != 4) return std::nullopt;

    int received_fd = -1;
    for (struct cmsghdr* cmsg = CMSG_FIRSTHDR(&msg); cmsg; cmsg = CMSG_NXTHDR(&msg, cmsg)) {
      if (cmsg->cmsg_level == SOL_SOCKET && cmsg->cmsg_type == SCM_RIGHTS) {
        received_fd = *reinterpret_cast<int*>(CMSG_DATA(cmsg));
        break;
      }
    }
    if (received_fd < 0) return std::nullopt;

    std::uint32_t payload_len = (static_cast<std::uint32_t>(static_cast<unsigned char>(len_buf[0])) << 24) |
                                (static_cast<std::uint32_t>(static_cast<unsigned char>(len_buf[1])) << 16) |
                                (static_cast<std::uint32_t>(static_cast<unsigned char>(len_buf[2])) << 8) |
                                static_cast<std::uint32_t>(static_cast<unsigned char>(len_buf[3]));
    if (payload_len > 1024 * 1024) {
      close(received_fd);
      return std::nullopt;
    }
    state.pending_fd = received_fd;
    state.payload_len = payload_len;
    state.payload.clear();
    state.payload.reserve(payload_len);
  }

  while (state.payload.size() < state.payload_len) {
    std::uint8_t buf[4096];
    size_t to_read = state.payload_len - state.payload.size();
    if (to_read > sizeof(buf)) to_read = sizeof(buf);
    ssize_t n = recv(socket_fd, buf, to_read, MSG_DONTWAIT | MSG_NOSIGNAL);
    if (n < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) return std::nullopt;
      close(state.pending_fd);
      state.pending_fd = -1;
      state.payload_len = 0;
      state.payload.clear();
      return std::nullopt;
    }
    if (n == 0) return std::nullopt;
    state.payload.insert(state.payload.end(), buf, buf + n);
  }

  int fd = state.pending_fd;
  state.pending_fd = -1;
  state.payload_len = 0;
  std::vector<std::uint8_t> payload = std::move(state.payload);
  state.payload.clear();
  return std::make_pair(fd, std::move(payload));
}

}  // namespace server
}  // namespace pgpooler
