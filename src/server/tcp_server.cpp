#include "server/tcp_server.h"

#include "net/connection.h"
#include "net/socket_ops.h"
#include "protocol/serializer.h"

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <limits>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace forgekv::server {
namespace {

constexpr std::uint32_t kBaseConnectionEvents = EPOLLRDHUP;

protocol::Frame busy_response(const protocol::Frame& request) {
  return protocol::Frame{
      .message_namespace = protocol::Namespace::client,
      .message_type = protocol::MessageType::busy,
      .flags = 0U,
      .request_id = request.request_id,
      .payload = std::vector<std::byte>(4U, std::byte{0}),
  };
}

RequestMetadata request_metadata(const protocol::Frame& frame) {
  return RequestMetadata{
      .message_namespace = frame.message_namespace,
      .message_type = frame.message_type,
      .request_id = frame.request_id,
  };
}

protocol::Frame busy_response(const RequestMetadata& request) {
  return protocol::Frame{
      .message_namespace = protocol::Namespace::client,
      .message_type = protocol::MessageType::busy,
      .flags = 0U,
      .request_id = request.request_id,
      .payload = std::vector<std::byte>(4U, std::byte{0}),
  };
}

ResponseMetadata response_metadata(const protocol::Frame& frame) {
  ResponseMetadata result{.message_type = frame.message_type,
                          .error_code = std::nullopt};
  if (frame.message_type == protocol::MessageType::error &&
      frame.payload.size() >= 2U) {
    result.error_code = static_cast<std::uint16_t>(
        (std::to_integer<std::uint16_t>(frame.payload[0]) << 8U) |
        std::to_integer<std::uint16_t>(frame.payload[1]));
  }
  return result;
}

}  // namespace

class TcpServer::Impl final {
 public:
  Impl(net::ReactorConfig config, Handler handler,
       RejectionObserver rejection_observer,
       CompletionObserver completion_observer)
      : config_(std::move(config)),
        executor_(config_.worker_threads, config_.global_outstanding_work,
                   config_.global_queued_work_bytes, std::move(handler),
                   [this](Completion completion) {
                     post_completion(std::move(completion));
                   }),
        rejection_observer_(std::move(rejection_observer)),
        completion_observer_(std::move(completion_observer)) {}

  ~Impl() { stop(); }

  std::optional<std::string> start(const std::string_view bind_address,
                                   const std::uint16_t port) {
    if (const auto error = config_.validation_error(); error.has_value()) {
      return error;
    }

    std::unique_lock lock(lifecycle_mutex_);
    if (ever_started_) {
      return std::string("TcpServer instances can be started only once");
    }
    ever_started_ = true;
    startup_complete_ = false;
    startup_error_.reset();
    const std::string address(bind_address);
    reactor_thread_ = std::thread([this, address, port] { run(address, port); });
    lifecycle_condition_.wait(lock, [this] { return startup_complete_; });
    return startup_error_;
  }

  void stop() {
    std::thread reactor;
    {
      const std::lock_guard lock(lifecycle_mutex_);
      if (!reactor_thread_.joinable()) {
        executor_.stop();
        return;
      }
    }

    executor_.stop();
    stop_requested_.store(true, std::memory_order_release);
    wake_reactor();

    {
      const std::lock_guard lock(lifecycle_mutex_);
      reactor = std::move(reactor_thread_);
    }
    reactor.join();
  }

  std::uint16_t bound_port() const noexcept {
    return bound_port_.load(std::memory_order_acquire);
  }

  net::MetricsSnapshot metrics() const noexcept { return metrics_.snapshot(); }

 private:
  void signal_startup(std::optional<std::string> error) {
    {
      const std::lock_guard lock(lifecycle_mutex_);
      startup_error_ = std::move(error);
      startup_complete_ = true;
    }
    lifecycle_condition_.notify_all();
  }

  void run(const std::string& bind_address, const std::uint16_t port) {
    const auto listener = net::create_listener(bind_address, port);
    if (!listener.ok()) {
      signal_startup(listener.error);
      return;
    }
    listener_fd_ = listener.fd;
    bound_port_.store(listener.port, std::memory_order_release);

    epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) {
      const auto error = std::string("epoll_create1: ") + std::strerror(errno);
      cleanup_descriptors();
      signal_startup(error);
      return;
    }
    wake_fd_ = ::eventfd(0U, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wake_fd_ < 0) {
      const auto error = std::string("eventfd: ") + std::strerror(errno);
      cleanup_descriptors();
      signal_startup(error);
      return;
    }
    if (!add_epoll_fd(listener_fd_, EPOLLIN) || !add_epoll_fd(wake_fd_, EPOLLIN)) {
      const auto error = std::string("epoll_ctl add: ") + std::strerror(errno);
      cleanup_descriptors();
      signal_startup(error);
      return;
    }

    signal_startup(std::nullopt);
    std::vector<epoll_event> events(config_.max_events_per_wait);
    while (!stop_requested_.load(std::memory_order_acquire)) {
      const auto count = ::epoll_wait(epoll_fd_, events.data(),
                                      static_cast<int>(events.size()), 100);
      if (count < 0) {
        if (errno == EINTR) {
          continue;
        }
        break;
      }
      for (int index = 0; index < count; ++index) {
        handle_event(events[static_cast<std::size_t>(index)]);
      }
    }

    process_completions();
    close_all_connections();
    cleanup_descriptors();
  }

  bool add_epoll_fd(const int fd, const std::uint32_t events) const noexcept {
    epoll_event event{};
    event.events = events;
    event.data.fd = fd;
    return ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &event) == 0;
  }

  void handle_event(const epoll_event& event) {
    const int fd = event.data.fd;
    if (fd == listener_fd_) {
      accept_connections();
      return;
    }
    if (fd == wake_fd_) {
      drain_wake_fd();
      process_completions();
      return;
    }

    if ((event.events & (EPOLLERR | EPOLLHUP)) != 0U) {
      close_connection(fd);
      return;
    }
    if ((event.events & EPOLLIN) != 0U) {
      read_connection(fd);
    }
    if (connections_.find(fd) == connections_.end()) {
      return;
    }
    if ((event.events & EPOLLOUT) != 0U) {
      write_connection(fd);
    }
    if (connections_.find(fd) != connections_.end() &&
        (event.events & EPOLLRDHUP) != 0U && (event.events & EPOLLIN) == 0U) {
      close_connection(fd);
    }
  }

  void accept_connections() {
    while (true) {
      const int fd = ::accept4(listener_fd_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
      if (fd < 0) {
        if (errno == EINTR) {
          continue;
        }
        return;
      }
      if (connections_.size() >= config_.max_connections) {
        ::close(fd);
        continue;
      }
      if (!net::set_tcp_nodelay(fd)) {
        ::close(fd);
        continue;
      }

      const net::ConnectionToken token{.id = next_connection_id_++,
                                       .generation = next_generation_++};
      net::Connection connection;
      connection.fd = fd;
      connection.token = token;
      connections_.emplace(fd, std::move(connection));
      token_to_fd_.emplace(token.id, fd);
      metrics_.connection_opened();
      if (!add_epoll_fd(fd, kBaseConnectionEvents | EPOLLIN)) {
        close_connection(fd);
        continue;
      }
    }
  }

  void read_connection(const int fd) {
    auto iterator = connections_.find(fd);
    if (iterator == connections_.end()) {
      return;
    }

    std::array<std::byte, 64U * 1024U> buffer{};
    std::size_t budget = config_.read_budget_per_event;
    while (budget > 0U && !iterator->second.read_paused) {
      const auto requested = std::min(buffer.size(), budget);
      const auto count = ::recv(fd, buffer.data(), requested, 0);
      if (count > 0) {
        const auto received = static_cast<std::size_t>(count);
        budget -= received;
        metrics_.add_bytes_read(received);
        iterator->second.last_activity = std::chrono::steady_clock::now();
        const auto before = iterator->second.parser.buffered_bytes();
        auto batch = iterator->second.parser.consume(
            std::span<const std::byte>(buffer).first(received));
        const auto after = iterator->second.parser.buffered_bytes();
        adjust_read_buffer_metrics(before, after);
        if (!batch.ok() || after > config_.max_buffered_input_per_connection) {
          close_connection(fd);
          return;
        }
        for (auto& frame : batch.frames) {
          if (!dispatch(iterator->second, std::move(frame))) {
            if (connections_.find(fd) == connections_.end()) {
              return;
            }
          }
        }
        iterator = connections_.find(fd);
        if (iterator == connections_.end()) {
          return;
        }
        continue;
      }
      if (count == 0) {
        close_connection(fd);
        return;
      }
      if (errno == EINTR) {
        continue;
      }
      if (net::would_block(errno)) {
        return;
      }
      close_connection(fd);
      return;
    }
  }

  bool dispatch(net::Connection& connection, protocol::Frame frame) {
    const auto wire_bytes = protocol::kHeaderSize + frame.payload.size();
    if (wire_bytes > config_.max_request_size) {
      close_connection(connection.fd);
      return false;
    }

    if (connection.pending_request_count >=
            config_.max_requests_in_flight_per_connection ||
        global_outstanding_ >= config_.global_outstanding_work) {
      observe_rejection(request_metadata(frame));
      const auto queued = queue_response(connection, busy_response(frame));
      set_backpressured(connection, true);
      return queued;
    }

    Request request{.token = connection.token,
                    .frame = std::move(frame),
                    .wire_bytes = wire_bytes};
    if (!executor_.try_submit(std::move(request))) {
      observe_rejection(request_metadata(request.frame));
      const auto queued = queue_response(connection, busy_response(request.frame));
      set_backpressured(connection, true);
      return queued;
    }
    ++connection.pending_request_count;
    ++global_outstanding_;
    metrics_.request_started();
    if (connection.pending_request_count >=
        config_.max_requests_in_flight_per_connection) {
      set_backpressured(connection, true);
    }
    return true;
  }

  void observe_rejection(const RequestMetadata& request) noexcept {
    metrics_.request_rejected();
    if (!rejection_observer_) {
      return;
    }
    try {
      rejection_observer_(request);
    } catch (...) {
      // Telemetry cannot change overload behavior.
    }
  }

  bool queue_response(net::Connection& connection, const protocol::Frame& frame) {
    auto serialized = protocol::serialize(frame);
    if (!serialized.ok() ||
        serialized.bytes.size() >
            config_.max_buffered_output_per_connection -
                std::min(connection.buffered_output_bytes,
                         config_.max_buffered_output_per_connection)) {
      close_connection(connection.fd);
      return false;
    }
    connection.buffered_output_bytes += serialized.bytes.size();
    metrics_.add_write_buffer_bytes(serialized.bytes.size());
    connection.write_queue.push_back(std::move(serialized.bytes));
    update_interest(connection);
    return true;
  }

  void write_connection(const int fd) {
    auto iterator = connections_.find(fd);
    if (iterator == connections_.end()) {
      return;
    }
    auto& connection = iterator->second;
    std::size_t budget = config_.write_budget_per_event;
    while (!connection.write_queue.empty() && budget > 0U) {
      auto& front = connection.write_queue.front();
      const auto remaining = front.size() - connection.write_offset;
      const auto requested = std::min(remaining, budget);
      const auto count = ::send(fd, front.data() + connection.write_offset, requested,
                                MSG_NOSIGNAL);
      if (count > 0) {
        const auto sent = static_cast<std::size_t>(count);
        connection.write_offset += sent;
        connection.buffered_output_bytes -= sent;
        metrics_.remove_write_buffer_bytes(sent);
        metrics_.add_bytes_written(sent);
        budget -= sent;
        connection.last_activity = std::chrono::steady_clock::now();
        if (connection.write_offset == front.size()) {
          connection.write_queue.pop_front();
          connection.write_offset = 0U;
        }
        continue;
      }
      if (count < 0 && errno == EINTR) {
        continue;
      }
      if (count < 0 && net::would_block(errno)) {
        break;
      }
      close_connection(fd);
      return;
    }
    update_interest(connection);
  }

  void post_completion(Completion completion) {
    const auto completion_bytes =
        protocol::kHeaderSize + completion.frame.payload.size();
    bool rejected = false;
    const auto request = completion.request;
    ResponseMetadata response;
    {
      const std::lock_guard lock(completion_mutex_);
      if (completion_bytes > config_.global_queued_work_bytes -
                                 std::min(completion_bytes_,
                                          config_.global_queued_work_bytes)) {
        completion.frame = busy_response(completion.request);
        rejected = true;
      }
      response = response_metadata(completion.frame);
      completion_bytes_ += protocol::kHeaderSize + completion.frame.payload.size();
      completions_.push_back(std::move(completion));
    }
    if (rejected) {
      metrics_.request_rejected();
    }
    if (completion_observer_) {
      try {
        completion_observer_(request, response);
      } catch (...) {
        // Telemetry cannot change response delivery.
      }
    }
    wake_reactor();
  }

  void process_completions() {
    std::deque<Completion> completions;
    {
      const std::lock_guard lock(completion_mutex_);
      completions.swap(completions_);
      completion_bytes_ = 0U;
    }

    for (auto& completion : completions) {
      if (global_outstanding_ > 0U) {
        --global_outstanding_;
        metrics_.request_finished();
      }
      const auto fd_iterator = token_to_fd_.find(completion.token.id);
      if (fd_iterator == token_to_fd_.end()) {
        continue;
      }
      auto connection_iterator = connections_.find(fd_iterator->second);
      if (connection_iterator == connections_.end() ||
          connection_iterator->second.token != completion.token) {
        continue;
      }
      auto& connection = connection_iterator->second;
      if (connection.pending_request_count > 0U) {
        --connection.pending_request_count;
      }
      if (!queue_response(connection, completion.frame)) {
        continue;
      }
      if (connection.backpressured &&
          connection.pending_request_count <
              config_.max_requests_in_flight_per_connection &&
          global_outstanding_ < config_.global_outstanding_work) {
        set_backpressured(connection, false);
      }
    }
  }

  void set_backpressured(net::Connection& connection, const bool backpressured) {
    if (connection.backpressured == backpressured) {
      return;
    }
    connection.backpressured = backpressured;
    connection.read_paused = backpressured;
    if (backpressured) {
      metrics_.backpressure_started();
    } else {
      metrics_.backpressure_ended();
    }
    update_interest(connection);
  }

  void update_interest(const net::Connection& connection) const noexcept {
    epoll_event event{};
    event.events = kBaseConnectionEvents;
    if (!connection.read_paused) {
      event.events |= EPOLLIN;
    }
    if (!connection.write_queue.empty()) {
      event.events |= EPOLLOUT;
    }
    event.data.fd = connection.fd;
    static_cast<void>(::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, connection.fd, &event));
  }

  void close_connection(const int fd) {
    const auto iterator = connections_.find(fd);
    if (iterator == connections_.end()) {
      return;
    }
    auto& connection = iterator->second;
    const auto input_bytes = connection.parser.buffered_bytes();
    if (input_bytes != 0U) {
      metrics_.remove_read_buffer_bytes(input_bytes);
    }
    if (connection.buffered_output_bytes != 0U) {
      metrics_.remove_write_buffer_bytes(connection.buffered_output_bytes);
    }
    if (connection.backpressured) {
      metrics_.backpressure_ended();
    }
    token_to_fd_.erase(connection.token.id);
    static_cast<void>(::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr));
    ::close(fd);
    connections_.erase(iterator);
    metrics_.connection_closed();
  }

  void close_all_connections() {
    while (!connections_.empty()) {
      close_connection(connections_.begin()->first);
    }
  }

  void adjust_read_buffer_metrics(const std::size_t before,
                                  const std::size_t after) noexcept {
    if (after > before) {
      metrics_.add_read_buffer_bytes(after - before);
    } else if (before > after) {
      metrics_.remove_read_buffer_bytes(before - after);
    }
  }

  void wake_reactor() const noexcept {
    if (wake_fd_ < 0) {
      return;
    }
    constexpr std::uint64_t value = 1U;
    const auto result = ::write(wake_fd_, &value, sizeof(value));
    static_cast<void>(result);
  }

  void drain_wake_fd() const noexcept {
    std::uint64_t value = 0U;
    while (::read(wake_fd_, &value, sizeof(value)) > 0) {
    }
  }

  void cleanup_descriptors() noexcept {
    if (listener_fd_ >= 0) {
      ::close(listener_fd_);
      listener_fd_ = -1;
    }
    if (wake_fd_ >= 0) {
      ::close(wake_fd_);
      wake_fd_ = -1;
    }
    if (epoll_fd_ >= 0) {
      ::close(epoll_fd_);
      epoll_fd_ = -1;
    }
  }

  net::ReactorConfig config_;
  net::Metrics metrics_;
  RequestExecutor executor_;
  RejectionObserver rejection_observer_;
  CompletionObserver completion_observer_;

  mutable std::mutex lifecycle_mutex_;
  std::condition_variable lifecycle_condition_;
  std::thread reactor_thread_;
  bool ever_started_{false};
  bool startup_complete_{false};
  std::optional<std::string> startup_error_;
  std::atomic<bool> stop_requested_{false};
  std::atomic<std::uint16_t> bound_port_{0U};

  int listener_fd_{-1};
  int epoll_fd_{-1};
  int wake_fd_{-1};
  std::uint64_t next_connection_id_{1U};
  std::uint64_t next_generation_{1U};
  std::size_t global_outstanding_{0U};
  std::unordered_map<int, net::Connection> connections_;
  std::unordered_map<std::uint64_t, int> token_to_fd_;

  std::mutex completion_mutex_;
  std::deque<Completion> completions_;
  std::size_t completion_bytes_{0U};
};

TcpServer::TcpServer(net::ReactorConfig config, Handler handler,
                     RejectionObserver rejection_observer,
                     CompletionObserver completion_observer)
    : impl_(std::make_unique<Impl>(std::move(config), std::move(handler),
                                   std::move(rejection_observer),
                                   std::move(completion_observer))) {}

TcpServer::~TcpServer() = default;

std::optional<std::string> TcpServer::start(const std::string_view bind_address,
                                            const std::uint16_t port) {
  return impl_->start(bind_address, port);
}

std::uint16_t TcpServer::bound_port() const noexcept {
  return impl_->bound_port();
}

net::MetricsSnapshot TcpServer::metrics() const noexcept {
  return impl_->metrics();
}

void TcpServer::stop() {
  impl_->stop();
}

}  // namespace forgekv::server
