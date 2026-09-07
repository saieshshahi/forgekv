#include "chaos/fault_proxy.h"

#include "net/socket_ops.h"

#include <arpa/inet.h>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <deque>
#include <fcntl.h>
#include <mutex>
#include <netdb.h>
#include <poll.h>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

namespace forgekv::chaos {
namespace {

using Clock = std::chrono::steady_clock;

struct Chunk final {
  std::vector<std::byte> bytes;
  std::size_t offset{};
  Clock::time_point due;
};

void close_descriptor(int& descriptor) noexcept {
  if (descriptor >= 0) {
    static_cast<void>(::shutdown(descriptor, SHUT_RDWR));
    static_cast<void>(::close(descriptor));
    descriptor = -1;
  }
}

bool set_nonblocking(const int descriptor) noexcept {
  const int flags = ::fcntl(descriptor, F_GETFL, 0);
  return flags >= 0 &&
         ::fcntl(descriptor, F_SETFL, flags | O_NONBLOCK) == 0;
}

int connect_destination(const FaultProxyConfig& config) {
  addrinfo hints{};
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  addrinfo* addresses = nullptr;
  const auto service = std::to_string(config.destination_port);
  if (::getaddrinfo(config.destination_host.c_str(), service.c_str(), &hints,
                    &addresses) != 0) {
    return -1;
  }
  int connected = -1;
  for (auto* address = addresses; address != nullptr;
       address = address->ai_next) {
    const int candidate = ::socket(address->ai_family,
                                   address->ai_socktype | SOCK_CLOEXEC,
                                   address->ai_protocol);
    if (candidate < 0) {
      continue;
    }
    if (::connect(candidate, address->ai_addr, address->ai_addrlen) == 0 &&
        set_nonblocking(candidate)) {
      connected = candidate;
      break;
    }
    static_cast<void>(::close(candidate));
  }
  ::freeaddrinfo(addresses);
  return connected;
}

}  // namespace

class FaultProxy::Impl final {
 public:
  explicit Impl(FaultProxyConfig config)
      : config_(std::move(config)), random_state_(config_.seed == 0U
                                                      ? 0x9E3779B97F4A7C15ULL
                                                      : config_.seed) {
    if (config_.destination_port == 0U ||
        config_.maximum_queued_bytes == 0U) {
      throw std::invalid_argument("invalid fault proxy configuration");
    }
  }

  ~Impl() { stop(); }

  ProxyStatus start() {
    std::lock_guard lock(lifecycle_mutex_);
    if (started_) {
      return {.error = "fault proxy can be started only once"};
    }
    auto listener =
        net::create_listener(config_.bind_host, config_.bind_port);
    if (!listener.ok()) {
      return {.error = listener.error};
    }
    int wake[2]{-1, -1};
    if (::pipe2(wake, O_NONBLOCK | O_CLOEXEC) != 0) {
      static_cast<void>(::close(listener.fd));
      return {.error = std::string("pipe2: ") + std::strerror(errno)};
    }
    listener_ = listener.fd;
    wake_read_ = wake[0];
    wake_write_ = wake[1];
    port_ = listener.port;
    started_ = true;
    thread_ = std::thread([this] { run(); });
    return {};
  }

  ProxyStatus set_policy(const LinkPolicy policy) {
    if (policy.loss_percent > 100U || policy.latency_ms > 60'000U ||
        policy.jitter_ms > 60'000U) {
      return {.error = "invalid link policy"};
    }
    {
      std::lock_guard lock(policy_mutex_);
      policy_ = policy;
      ++policy_generation_;
    }
    wake();
    return {};
  }

  LinkPolicy policy() const {
    std::lock_guard lock(policy_mutex_);
    return policy_;
  }

  std::uint16_t port() const noexcept { return port_; }

  void stop() noexcept {
    std::unique_lock lock(lifecycle_mutex_);
    if (!started_ || stopped_) {
      return;
    }
    stopped_ = true;
    wake();
    lock.unlock();
    if (thread_.joinable()) {
      thread_.join();
    }
    lock.lock();
    close_descriptor(listener_);
    close_descriptor(wake_read_);
    close_descriptor(wake_write_);
  }

 private:
  std::uint64_t random() noexcept {
    random_state_ ^= random_state_ >> 12U;
    random_state_ ^= random_state_ << 25U;
    random_state_ ^= random_state_ >> 27U;
    return random_state_ * 0x2545F4914F6CDD1DULL;
  }

  void wake() noexcept {
    if (wake_write_ < 0) {
      return;
    }
    const std::byte byte{1};
    [[maybe_unused]] const auto written = ::write(wake_write_, &byte, 1U);
  }

  LinkPolicy policy_snapshot(std::uint64_t& generation) const {
    std::lock_guard lock(policy_mutex_);
    generation = policy_generation_;
    return policy_;
  }

  void close_stream() noexcept {
    close_descriptor(upstream_);
    close_descriptor(downstream_);
    to_upstream_.clear();
    to_downstream_.clear();
    queued_upstream_ = 0U;
    queued_downstream_ = 0U;
  }

  void drain_wake() noexcept {
    std::byte bytes[64]{};
    while (::read(wake_read_, bytes, sizeof(bytes)) > 0) {
    }
  }

  bool should_drop(const LinkPolicy& policy) noexcept {
    return policy.loss_percent != 0U &&
           random() % 100U < policy.loss_percent;
  }

  Clock::time_point due_time(const LinkPolicy& policy) noexcept {
    std::uint64_t delay = policy.latency_ms;
    if (policy.jitter_ms != 0U) {
      delay += random() % (static_cast<std::uint64_t>(policy.jitter_ms) + 1U);
    }
    return Clock::now() + std::chrono::milliseconds(delay);
  }

  bool read_chunk(const int source, std::deque<Chunk>& queue,
                  std::size_t& queued, const LinkPolicy& policy) {
    std::vector<std::byte> bytes(64U * 1024U);
    const auto count = ::recv(source, bytes.data(), bytes.size(), 0);
    if (count == 0) {
      return false;
    }
    if (count < 0) {
      return net::would_block(errno) || errno == EINTR;
    }
    bytes.resize(static_cast<std::size_t>(count));
    if (should_drop(policy) ||
        bytes.size() > config_.maximum_queued_bytes -
                           std::min(queued, config_.maximum_queued_bytes)) {
      return false;
    }
    queued += bytes.size();
    queue.push_back(
        Chunk{.bytes = std::move(bytes), .offset = 0U, .due = due_time(policy)});
    return true;
  }

  bool write_chunk(const int destination, std::deque<Chunk>& queue,
                   std::size_t& queued) {
    if (queue.empty() || queue.front().due > Clock::now()) {
      return true;
    }
    auto& chunk = queue.front();
    const auto count = ::send(destination, chunk.bytes.data() + chunk.offset,
                              chunk.bytes.size() - chunk.offset, MSG_NOSIGNAL);
    if (count < 0) {
      return net::would_block(errno) || errno == EINTR;
    }
    if (count == 0) {
      return false;
    }
    const auto sent = static_cast<std::size_t>(count);
    chunk.offset += sent;
    queued -= sent;
    if (chunk.offset == chunk.bytes.size()) {
      queue.pop_front();
    }
    return true;
  }

  int poll_timeout() const noexcept {
    auto due = Clock::time_point::max();
    if (!to_upstream_.empty()) {
      due = std::min(due, to_upstream_.front().due);
    }
    if (!to_downstream_.empty()) {
      due = std::min(due, to_downstream_.front().due);
    }
    if (due == Clock::time_point::max()) {
      return 100;
    }
    const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
        due - Clock::now());
    if (remaining.count() <= 0) {
      return 0;
    }
    return static_cast<int>(std::min<std::int64_t>(remaining.count(), 100));
  }

  void accept_stream(const LinkPolicy& policy) {
    const int accepted =
        ::accept4(listener_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (accepted < 0) {
      return;
    }
    if (policy.partitioned || upstream_ >= 0) {
      static_cast<void>(::close(accepted));
      return;
    }
    const int destination = connect_destination(config_);
    if (destination < 0) {
      static_cast<void>(::close(accepted));
      return;
    }
    upstream_ = accepted;
    downstream_ = destination;
  }

  void run() noexcept {
    std::uint64_t seen_generation = 0U;
    while (true) {
      {
        std::lock_guard lock(lifecycle_mutex_);
        if (stopped_) {
          break;
        }
      }
      std::uint64_t generation = 0U;
      const auto policy = policy_snapshot(generation);
      if (generation != seen_generation) {
        seen_generation = generation;
        if (policy.partitioned) {
          close_stream();
        }
      }

      std::vector<pollfd> events;
      events.push_back(pollfd{.fd = listener_,
                              .events = static_cast<short>(
                                  upstream_ < 0 ? POLLIN : 0),
                              .revents = 0});
      events.push_back(
          pollfd{.fd = wake_read_, .events = POLLIN, .revents = 0});
      if (upstream_ >= 0) {
        short requested = POLLIN;
        if (!to_upstream_.empty() && to_upstream_.front().due <= Clock::now()) {
          requested = static_cast<short>(requested | POLLOUT);
        }
        events.push_back(
            pollfd{.fd = upstream_, .events = requested, .revents = 0});
      }
      if (downstream_ >= 0) {
        short requested = POLLIN;
        if (!to_downstream_.empty() &&
            to_downstream_.front().due <= Clock::now()) {
          requested = static_cast<short>(requested | POLLOUT);
        }
        events.push_back(
            pollfd{.fd = downstream_, .events = requested, .revents = 0});
      }

      const auto ready = ::poll(events.data(), events.size(), poll_timeout());
      if (ready < 0 && errno != EINTR) {
        break;
      }
      if ((events[1].revents & POLLIN) != 0) {
        drain_wake();
      }
      if ((events[0].revents & POLLIN) != 0) {
        accept_stream(policy);
      }
      if (upstream_ < 0 || downstream_ < 0 || events.size() < 4U) {
        continue;
      }
      const auto invalid = POLLERR | POLLHUP | POLLNVAL;
      if ((events[2].revents & invalid) != 0 ||
          (events[3].revents & invalid) != 0 ||
          ((events[2].revents & POLLIN) != 0 &&
           !read_chunk(upstream_, to_downstream_, queued_downstream_, policy)) ||
          ((events[3].revents & POLLIN) != 0 &&
           !read_chunk(downstream_, to_upstream_, queued_upstream_, policy)) ||
          ((events[2].revents & POLLOUT) != 0 &&
           !write_chunk(upstream_, to_upstream_, queued_upstream_)) ||
          ((events[3].revents & POLLOUT) != 0 &&
           !write_chunk(downstream_, to_downstream_, queued_downstream_))) {
        close_stream();
      }
    }
    close_stream();
  }

  FaultProxyConfig config_;
  mutable std::mutex lifecycle_mutex_;
  mutable std::mutex policy_mutex_;
  LinkPolicy policy_;
  std::uint64_t policy_generation_{};
  std::uint64_t random_state_;
  bool started_{};
  bool stopped_{};
  int listener_{-1};
  int wake_read_{-1};
  int wake_write_{-1};
  std::uint16_t port_{};
  std::thread thread_;
  int upstream_{-1};
  int downstream_{-1};
  std::deque<Chunk> to_upstream_;
  std::deque<Chunk> to_downstream_;
  std::size_t queued_upstream_{};
  std::size_t queued_downstream_{};
};

FaultProxy::FaultProxy(FaultProxyConfig config)
    : impl_(std::make_unique<Impl>(std::move(config))) {}

FaultProxy::~FaultProxy() = default;

ProxyStatus FaultProxy::start() { return impl_->start(); }

ProxyStatus FaultProxy::set_policy(const LinkPolicy policy) {
  return impl_->set_policy(policy);
}

LinkPolicy FaultProxy::policy() const { return impl_->policy(); }

std::uint16_t FaultProxy::port() const noexcept { return impl_->port(); }

void FaultProxy::stop() noexcept { impl_->stop(); }

}  // namespace forgekv::chaos
