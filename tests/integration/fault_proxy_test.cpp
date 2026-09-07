#include "chaos/fault_proxy.h"

#include <arpa/inet.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <thread>

#include <gtest/gtest.h>

namespace forgekv::chaos {
namespace {

class EchoServer final {
 public:
  EchoServer() {
    listener_ = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    EXPECT_GE(listener_, 0);
    const int enabled = 1;
    EXPECT_EQ(::setsockopt(listener_, SOL_SOCKET, SO_REUSEADDR, &enabled,
                           sizeof(enabled)),
              0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    EXPECT_EQ(::bind(listener_, reinterpret_cast<sockaddr*>(&address),
                     sizeof(address)),
              0);
    EXPECT_EQ(::listen(listener_, 4), 0);
    socklen_t size = sizeof(address);
    EXPECT_EQ(::getsockname(listener_, reinterpret_cast<sockaddr*>(&address),
                            &size),
              0);
    port_ = ntohs(address.sin_port);
    thread_ = std::thread([this] { run(); });
  }

  ~EchoServer() {
    stopping_.store(true, std::memory_order_release);
    ::shutdown(listener_, SHUT_RDWR);
    ::close(listener_);
    if (thread_.joinable()) {
      thread_.join();
    }
  }

  [[nodiscard]] std::uint16_t port() const noexcept { return port_; }

 private:
  void run() {
    while (!stopping_.load(std::memory_order_acquire)) {
      const int client = ::accept4(listener_, nullptr, nullptr, SOCK_CLOEXEC);
      if (client < 0) {
        continue;
      }
      std::array<std::byte, 4096> bytes{};
      while (!stopping_.load(std::memory_order_acquire)) {
        const auto count = ::recv(client, bytes.data(), bytes.size(), 0);
        if (count <= 0) {
          break;
        }
        std::size_t sent = 0;
        while (sent < static_cast<std::size_t>(count)) {
          const auto result = ::send(client, bytes.data() + sent,
                                     static_cast<std::size_t>(count) - sent,
                                     MSG_NOSIGNAL);
          if (result <= 0) {
            break;
          }
          sent += static_cast<std::size_t>(result);
        }
      }
      ::close(client);
    }
  }

  int listener_{-1};
  std::uint16_t port_{};
  std::atomic<bool> stopping_{};
  std::thread thread_;
};

int connect_to(const std::uint16_t port) {
  const int descriptor = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (descriptor < 0) {
    return -1;
  }
  timeval timeout{.tv_sec = 1, .tv_usec = 0};
  static_cast<void>(::setsockopt(descriptor, SOL_SOCKET, SO_RCVTIMEO, &timeout,
                                 sizeof(timeout)));
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = htons(port);
  if (::connect(descriptor, reinterpret_cast<sockaddr*>(&address),
                sizeof(address)) != 0) {
    ::close(descriptor);
    return -1;
  }
  return descriptor;
}

std::string round_trip(const int descriptor, const std::string& payload) {
  if (::send(descriptor, payload.data(), payload.size(), MSG_NOSIGNAL) !=
      static_cast<ssize_t>(payload.size())) {
    return {};
  }
  std::string response(payload.size(), '\0');
  const auto count = ::recv(descriptor, response.data(), response.size(),
                            MSG_WAITALL);
  return count == static_cast<ssize_t>(response.size()) ? response
                                                        : std::string{};
}

bool peer_closed(const int descriptor) {
  pollfd event{.fd = descriptor, .events = POLLIN | POLLHUP, .revents = 0};
  if (::poll(&event, 1, 1000) <= 0) {
    return false;
  }
  std::byte byte{};
  return ::recv(descriptor, &byte, 1, 0) == 0;
}

FaultProxyConfig proxy_config(const std::uint16_t destination_port) {
  return FaultProxyConfig{.destination_host = "127.0.0.1",
                          .destination_port = destination_port,
                          .seed = 12345};
}

TEST(FaultProxyTest, PartitionClosesExistingStreamAndHealReconnects) {
  EchoServer destination;
  FaultProxy proxy(proxy_config(destination.port()));
  ASSERT_TRUE(proxy.start().ok());
  const int client = connect_to(proxy.port());
  ASSERT_GE(client, 0);
  EXPECT_EQ(round_trip(client, "before"), "before");
  ASSERT_TRUE(proxy.set_policy(LinkPolicy{.partitioned = true}).ok());
  EXPECT_TRUE(peer_closed(client));
  ::close(client);
  ASSERT_TRUE(proxy.set_policy(LinkPolicy{}).ok());
  const int healed = connect_to(proxy.port());
  ASSERT_GE(healed, 0);
  EXPECT_EQ(round_trip(healed, "after"), "after");
  ::close(healed);
  proxy.stop();
  proxy.stop();
}

TEST(FaultProxyTest, FixedLatencyDelaysBothDirectionsWithinBounds) {
  EchoServer destination;
  FaultProxy proxy(proxy_config(destination.port()));
  ASSERT_TRUE(proxy.start().ok());
  ASSERT_TRUE(proxy.set_policy(LinkPolicy{.latency_ms = 30}).ok());
  const int client = connect_to(proxy.port());
  ASSERT_GE(client, 0);
  const auto start = std::chrono::steady_clock::now();
  EXPECT_EQ(round_trip(client, "delayed"), "delayed");
  const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - start);
  EXPECT_GE(elapsed.count(), 50);
  EXPECT_LT(elapsed.count(), 1000);
  ::close(client);
}

TEST(FaultProxyTest, QueueOverflowClosesStreamAndStopJoins) {
  EchoServer destination;
  auto config = proxy_config(destination.port());
  config.maximum_queued_bytes = 8;
  FaultProxy proxy(config);
  ASSERT_TRUE(proxy.start().ok());
  ASSERT_TRUE(proxy.set_policy(LinkPolicy{.latency_ms = 500}).ok());
  const int client = connect_to(proxy.port());
  ASSERT_GE(client, 0);
  const std::string payload(32, 'x');
  static_cast<void>(::send(client, payload.data(), payload.size(), MSG_NOSIGNAL));
  EXPECT_TRUE(peer_closed(client));
  ::close(client);
}

TEST(FaultProxyTest, SeededHundredPercentLossClosesStream) {
  EchoServer destination;
  FaultProxy proxy(proxy_config(destination.port()));
  ASSERT_TRUE(proxy.start().ok());
  ASSERT_TRUE(proxy.set_policy(LinkPolicy{.loss_percent = 100}).ok());
  const int client = connect_to(proxy.port());
  ASSERT_GE(client, 0);
  static_cast<void>(::send(client, "lost", 4, MSG_NOSIGNAL));
  EXPECT_TRUE(peer_closed(client));
  ::close(client);
}

TEST(FaultProxyTest, HealthyPolicyQueuesConcurrentStreamsWithoutDropping) {
  EchoServer destination;
  FaultProxy proxy(proxy_config(destination.port()));
  ASSERT_TRUE(proxy.start().ok());
  const int first = connect_to(proxy.port());
  ASSERT_GE(first, 0);
  EXPECT_EQ(round_trip(first, "first"), "first");

  const int second = connect_to(proxy.port());
  ASSERT_GE(second, 0);
  ASSERT_EQ(::send(second, "second", 6, MSG_NOSIGNAL), 6);
  ::close(first);
  std::array<char, 6> response{};
  EXPECT_EQ(::recv(second, response.data(), response.size(), MSG_WAITALL), 6);
  EXPECT_EQ(std::string(response.data(), response.size()), "second");
  ::close(second);
}

TEST(FaultProxyTest, RejectsInvalidPolicyInsteadOfClaimingItWasApplied) {
  EchoServer destination;
  FaultProxy proxy(proxy_config(destination.port()));
  ASSERT_TRUE(proxy.start().ok());
  EXPECT_FALSE(proxy.set_policy(LinkPolicy{.latency_ms = 60'001U}).ok());
  EXPECT_FALSE(proxy.set_policy(LinkPolicy{.jitter_ms = 60'001U}).ok());
  EXPECT_FALSE(proxy.set_policy(LinkPolicy{.loss_percent = 101U}).ok());
}

}  // namespace
}  // namespace forgekv::chaos
