#include "net/socket_ops.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <string>

namespace forgekv::net {
namespace {

ListenerResult failure(const int fd, const char* operation) {
  const auto saved_error = errno;
  if (fd >= 0) {
    ::close(fd);
  }
  return {.fd = -1,
          .port = 0U,
          .error = std::string(operation) + ": " + std::strerror(saved_error)};
}

}  // namespace

ListenerResult create_listener(const std::string_view address, const std::uint16_t port) {
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  if (fd < 0) {
    return failure(-1, "socket");
  }

  const int enabled = 1;
  if (::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled)) != 0) {
    return failure(fd, "setsockopt(SO_REUSEADDR)");
  }

  sockaddr_in socket_address{};
  socket_address.sin_family = AF_INET;
  socket_address.sin_port = htons(port);
  const std::string address_text(address);
  if (::inet_pton(AF_INET, address_text.c_str(), &socket_address.sin_addr) != 1) {
    ::close(fd);
    return {.fd = -1, .port = 0U, .error = "invalid IPv4 bind address"};
  }

  if (::bind(fd, reinterpret_cast<const sockaddr*>(&socket_address),
             sizeof(socket_address)) != 0) {
    return failure(fd, "bind");
  }
  if (::listen(fd, SOMAXCONN) != 0) {
    return failure(fd, "listen");
  }

  socklen_t length = sizeof(socket_address);
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&socket_address), &length) != 0) {
    return failure(fd, "getsockname");
  }
  return {.fd = fd, .port = ntohs(socket_address.sin_port), .error = {}};
}

bool set_tcp_nodelay(const int fd) noexcept {
  const int enabled = 1;
  return ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &enabled, sizeof(enabled)) == 0;
}

bool would_block(const int error_number) noexcept {
  return error_number == EAGAIN || error_number == EWOULDBLOCK;
}

}  // namespace forgekv::net
