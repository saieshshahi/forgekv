#include "server/admin_server.h"

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cstddef>
#include <string>

namespace forgekv::server {
namespace {

std::string exchange(const std::uint16_t port, const std::string& request) {
  const int descriptor = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (descriptor < 0) {
    return {};
  }
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(port);
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  if (::connect(descriptor, reinterpret_cast<const sockaddr*>(&address),
                sizeof(address)) != 0) {
    ::close(descriptor);
    return {};
  }
  static_cast<void>(::send(descriptor, request.data(), request.size(), 0));
  static_cast<void>(::shutdown(descriptor, SHUT_WR));
  std::string response;
  std::array<char, 4096> bytes{};
  while (true) {
    const auto count = ::recv(descriptor, bytes.data(), bytes.size(), 0);
    if (count <= 0) {
      break;
    }
    response.append(bytes.data(), static_cast<std::size_t>(count));
  }
  ::close(descriptor);
  return response;
}

TEST(AdminServerTest, ServesBoundedHttpAndStopsDeterministically) {
  AdminServer server([](const std::string_view path) {
    if (path == "/health") {
      return AdminResponse{.status = 200,
                           .content_type = "application/json",
                           .body = "{\"status\":\"healthy\"}\n"};
    }
    return AdminResponse{.status = 404,
                         .content_type = "text/plain; charset=utf-8",
                         .body = "not found\n"};
  });
  ASSERT_FALSE(server.start("127.0.0.1", 0).has_value());
  ASSERT_NE(server.bound_port(), 0);

  const auto healthy = exchange(
      server.bound_port(),
      "GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
  EXPECT_NE(healthy.find("HTTP/1.1 200 OK"), std::string::npos);
  EXPECT_NE(healthy.find("Content-Type: application/json"), std::string::npos);
  EXPECT_NE(healthy.find("{\"status\":\"healthy\"}"), std::string::npos);

  const auto method = exchange(
      server.bound_port(),
      "POST /health HTTP/1.1\r\nHost: localhost\r\n\r\n");
  EXPECT_NE(method.find("HTTP/1.1 405 Method Not Allowed"),
            std::string::npos);

  const auto malformed_version = exchange(
      server.bound_port(),
      "GET /health NOT-HTTP\r\nHost: localhost\r\nConnection: close\r\n\r\n");
  EXPECT_NE(malformed_version.find("HTTP/1.1 400 Bad Request"),
            std::string::npos);

  const auto oversized = exchange(
      server.bound_port(), "GET /health HTTP/1.1\r\nX: " +
                               std::string(5000, 'x') + "\r\n\r\n");
  EXPECT_NE(oversized.find("HTTP/1.1 400 Bad Request"), std::string::npos);

  server.stop();
  server.stop();
}

}  // namespace
}  // namespace forgekv::server
