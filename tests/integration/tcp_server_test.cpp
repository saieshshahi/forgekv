#include "server/tcp_server.h"

#include "protocol/parser.h"
#include "protocol/serializer.h"

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <span>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace forgekv::server {
namespace {

using namespace std::chrono_literals;

class Socket final {
 public:
  explicit Socket(const int fd = -1) : fd_(fd) {}
  ~Socket() {
    if (fd_ >= 0) {
      ::close(fd_);
    }
  }
  Socket(const Socket&) = delete;
  Socket& operator=(const Socket&) = delete;
  Socket(Socket&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}
  int get() const noexcept { return fd_; }

 private:
  int fd_;
};

std::vector<std::byte> text_bytes(const std::string_view text) {
  const auto* begin = reinterpret_cast<const std::byte*>(text.data());
  return {begin, begin + text.size()};
}

protocol::Frame ping(const std::uint64_t id, const std::string_view payload) {
  return protocol::Frame{
      .message_namespace = protocol::Namespace::client,
      .message_type = protocol::MessageType::ping,
      .flags = 0U,
      .request_id = id,
      .payload = text_bytes(payload),
  };
}

protocol::Frame ping_handler(const protocol::Frame& request) {
  auto response = request;
  response.message_type = protocol::MessageType::ok;
  return response;
}

Socket connect_to(const std::uint16_t port) {
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  EXPECT_GE(fd, 0);
  if (fd < 0) {
    return Socket{};
  }

  timeval timeout{.tv_sec = 2, .tv_usec = 0};
  EXPECT_EQ(::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)), 0);

  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(port);
  EXPECT_EQ(::inet_pton(AF_INET, "127.0.0.1", &address.sin_addr), 1);
  EXPECT_EQ(::connect(fd, reinterpret_cast<const sockaddr*>(&address), sizeof(address)), 0);
  return Socket{fd};
}

void send_all(const int fd, const std::span<const std::byte> bytes) {
  std::size_t offset = 0U;
  while (offset < bytes.size()) {
    const auto result = ::send(fd, bytes.data() + offset, bytes.size() - offset, MSG_NOSIGNAL);
    ASSERT_GT(result, 0);
    offset += static_cast<std::size_t>(result);
  }
}

std::vector<protocol::Frame> receive_frames(const int fd, const std::size_t expected) {
  protocol::Parser parser;
  std::vector<protocol::Frame> frames;
  std::array<std::byte, 4096U> buffer{};
  while (frames.size() < expected) {
    const auto count = ::recv(fd, buffer.data(), buffer.size(), 0);
    EXPECT_GT(count, 0);
    if (count <= 0) {
      break;
    }
    auto batch = parser.consume(
        std::span<const std::byte>(buffer).first(static_cast<std::size_t>(count)));
    EXPECT_TRUE(batch.ok());
    frames.insert(frames.end(), std::make_move_iterator(batch.frames.begin()),
                  std::make_move_iterator(batch.frames.end()));
  }
  return frames;
}

TEST(TcpServer, PingRoundTripPreservesCorrelationAndUpdatesMetrics) {
  TcpServer server(net::ReactorConfig{}, ping_handler);
  ASSERT_FALSE(server.start("127.0.0.1", 0U).has_value());
  const auto socket = connect_to(server.bound_port());
  const auto request = ping(17U, "hello");
  const auto wire = protocol::serialize(request).bytes;

  send_all(socket.get(), wire);
  const auto responses = receive_frames(socket.get(), 1U);

  ASSERT_EQ(responses.size(), 1U);
  EXPECT_EQ(responses.front().message_type, protocol::MessageType::ok);
  EXPECT_EQ(responses.front().request_id, request.request_id);
  EXPECT_EQ(responses.front().payload, request.payload);
  const auto metrics = server.metrics();
  EXPECT_EQ(metrics.active_connections, 1U);
  EXPECT_EQ(metrics.accepted_connections_total, 1U);
  EXPECT_GE(metrics.bytes_read_total, wire.size());
  EXPECT_GT(metrics.bytes_written_total, 0U);
  server.stop();
}

TEST(TcpServer, HandlesFragmentedAndPipelinedFrames) {
  TcpServer server(net::ReactorConfig{}, ping_handler);
  ASSERT_FALSE(server.start("127.0.0.1", 0U).has_value());
  const auto socket = connect_to(server.bound_port());

  const auto fragmented = protocol::serialize(ping(1U, "fragmented")).bytes;
  for (const auto byte : fragmented) {
    send_all(socket.get(), std::span<const std::byte>(&byte, 1U));
  }
  ASSERT_EQ(receive_frames(socket.get(), 1U).front().request_id, 1U);

  auto pipelined = protocol::serialize(ping(2U, "two")).bytes;
  const auto third = protocol::serialize(ping(3U, "three")).bytes;
  pipelined.insert(pipelined.end(), third.begin(), third.end());
  send_all(socket.get(), pipelined);
  const auto responses = receive_frames(socket.get(), 2U);

  ASSERT_EQ(responses.size(), 2U);
  std::vector<std::uint64_t> response_ids{responses[0].request_id,
                                          responses[1].request_id};
  std::ranges::sort(response_ids);
  EXPECT_EQ(response_ids, (std::vector<std::uint64_t>{2U, 3U}));
  server.stop();
}

TEST(TcpServer, ReturnsBusyWhenPerConnectionWorkLimitIsReached) {
  net::ReactorConfig config;
  config.worker_threads = 1U;
  config.max_requests_in_flight_per_connection = 1U;
  config.global_outstanding_work = 1U;

  std::mutex gate_mutex;
  std::condition_variable gate_condition;
  bool entered = false;
  bool release = false;
  TcpServer server(config, [&](const protocol::Frame& request) {
    std::unique_lock lock(gate_mutex);
    entered = true;
    gate_condition.notify_all();
    gate_condition.wait(lock, [&] { return release; });
    return ping_handler(request);
  });
  ASSERT_FALSE(server.start("127.0.0.1", 0U).has_value());
  const auto socket = connect_to(server.bound_port());
  auto wire = protocol::serialize(ping(1U, "one")).bytes;
  const auto second = protocol::serialize(ping(2U, "two")).bytes;
  wire.insert(wire.end(), second.begin(), second.end());
  send_all(socket.get(), wire);

  {
    std::unique_lock lock(gate_mutex);
    ASSERT_TRUE(gate_condition.wait_for(lock, 2s, [&] { return entered; }));
    release = true;
  }
  gate_condition.notify_all();
  const auto responses = receive_frames(socket.get(), 2U);

  ASSERT_EQ(responses.size(), 2U);
  const auto busy_count = std::ranges::count_if(responses, [](const auto& frame) {
    return frame.message_type == protocol::MessageType::busy;
  });
  const auto ok_count = std::ranges::count_if(responses, [](const auto& frame) {
    return frame.message_type == protocol::MessageType::ok;
  });
  EXPECT_EQ(busy_count, 1);
  EXPECT_EQ(ok_count, 1);
  server.stop();
}

TEST(TcpServer, PeerCloseAndStopReleaseAllConnections) {
  TcpServer server(net::ReactorConfig{}, ping_handler);
  ASSERT_FALSE(server.start("127.0.0.1", 0U).has_value());
  {
    const auto socket = connect_to(server.bound_port());
    send_all(socket.get(), protocol::serialize(ping(1U, "bye")).bytes);
    EXPECT_EQ(receive_frames(socket.get(), 1U).size(), 1U);
  }

  for (int attempt = 0; attempt < 100 && server.metrics().active_connections != 0U;
       ++attempt) {
    std::this_thread::sleep_for(5ms);
  }
  EXPECT_EQ(server.metrics().active_connections, 0U);
  EXPECT_EQ(server.metrics().closed_connections_total, 1U);
  server.stop();
  server.stop();
}

}  // namespace
}  // namespace forgekv::server
