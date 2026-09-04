#include "cluster/node.h"

#include "cluster/codecs.h"
#include "cluster/observability.h"
#include "cluster/snapshot_codec.h"
#include "cluster/state_machine.h"
#include "common/logging.h"
#include "protocol/parser.h"
#include "protocol/serializer.h"
#include "protocol/wire.h"
#include "raft/persisted_raft_node.h"
#include "raft/snapshot_store.h"
#include "server/admin_server.h"
#include "server/tcp_server.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

namespace forgekv::cluster {
namespace {

using namespace std::chrono_literals;
namespace wire = forgekv::protocol::wire;

void write_u16(const std::span<std::byte> destination,
               const std::uint16_t value) {
  destination[0] = static_cast<std::byte>((value >> 8U) & 0xFFU);
  destination[1] = static_cast<std::byte>(value & 0xFFU);
}

protocol::Frame client_response(const protocol::Frame& request,
                                const protocol::MessageType type,
                                std::vector<std::byte> payload = {}) {
  return protocol::Frame{
      .message_namespace = protocol::Namespace::client,
      .message_type = type,
      .flags = 0,
      .request_id = request.request_id,
      .payload = std::move(payload),
  };
}

protocol::Frame error_response(const protocol::Frame& request,
                               const std::uint16_t code,
                               const std::string_view message) {
  const auto bounded = message.substr(0, 512U);
  std::vector<std::byte> payload(4U + bounded.size());
  write_u16(std::span{payload}.subspan(0, 2), code);
  write_u16(std::span{payload}.subspan(2, 2),
            static_cast<std::uint16_t>(bounded.size()));
  std::ranges::transform(
      bounded, payload.begin() + 4, [](const char character) {
        return static_cast<std::byte>(static_cast<unsigned char>(character));
      });
  return client_response(request, protocol::MessageType::error,
                         std::move(payload));
}

protocol::Frame busy_response(const protocol::Frame& request) {
  return client_response(request, protocol::MessageType::busy,
                         std::vector<std::byte>(4U, std::byte{0}));
}

protocol::Frame redirect_response(const protocol::Frame& request,
                                  const std::string& endpoint) {
  if (endpoint.size() > std::numeric_limits<std::uint16_t>::max()) {
    return busy_response(request);
  }
  std::vector<std::byte> payload(2U + endpoint.size());
  write_u16(std::span{payload}.first<2>(),
            static_cast<std::uint16_t>(endpoint.size()));
  std::ranges::transform(
      endpoint, payload.begin() + 2, [](const char character) {
        return static_cast<std::byte>(static_cast<unsigned char>(character));
      });
  return client_response(request, protocol::MessageType::redirect,
                         std::move(payload));
}

class Socket final {
 public:
  explicit Socket(const int descriptor = -1) : descriptor_(descriptor) {}
  ~Socket() {
    if (descriptor_ >= 0) {
      static_cast<void>(::close(descriptor_));
    }
  }
  Socket(const Socket&) = delete;
  Socket& operator=(const Socket&) = delete;
  Socket(Socket&& other) noexcept
      : descriptor_(std::exchange(other.descriptor_, -1)) {}
  [[nodiscard]] int get() const noexcept { return descriptor_; }
  [[nodiscard]] bool valid() const noexcept { return descriptor_ >= 0; }

 private:
  int descriptor_;
};

Socket connect_with_timeout(const std::string& host, const std::uint16_t port,
                            const std::uint32_t timeout_ms) {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;
  hints.ai_flags = AI_NUMERICHOST;
  addrinfo* addresses = nullptr;
  const auto service = std::to_string(port);
  if (::getaddrinfo(host.c_str(), service.c_str(), &hints, &addresses) != 0) {
    return Socket{};
  }
  std::unique_ptr<addrinfo, decltype(&::freeaddrinfo)> owned(addresses,
                                                             ::freeaddrinfo);
  for (auto* address = addresses; address != nullptr;
       address = address->ai_next) {
    Socket socket(::socket(address->ai_family,
                           address->ai_socktype | SOCK_CLOEXEC | SOCK_NONBLOCK,
                           address->ai_protocol));
    if (!socket.valid()) {
      continue;
    }
    if (::connect(socket.get(), address->ai_addr, address->ai_addrlen) != 0 &&
        errno != EINPROGRESS) {
      continue;
    }
    pollfd descriptor{.fd = socket.get(), .events = POLLOUT, .revents = 0};
    int polled = 0;
    do {
      polled = ::poll(&descriptor, 1, static_cast<int>(timeout_ms));
    } while (polled < 0 && errno == EINTR);
    int socket_error = 0;
    socklen_t error_size = sizeof(socket_error);
    if (polled <= 0 ||
        ::getsockopt(socket.get(), SOL_SOCKET, SO_ERROR, &socket_error,
                     &error_size) != 0 ||
        socket_error != 0) {
      continue;
    }
    const int flags = ::fcntl(socket.get(), F_GETFL, 0);
    if (flags < 0 ||
        ::fcntl(socket.get(), F_SETFL, flags & ~O_NONBLOCK) != 0) {
      continue;
    }
    timeval timeout{.tv_sec = static_cast<time_t>(timeout_ms / 1000U),
                    .tv_usec = static_cast<suseconds_t>(
                        (timeout_ms % 1000U) * 1000U)};
    if (::setsockopt(socket.get(), SOL_SOCKET, SO_RCVTIMEO, &timeout,
                     sizeof(timeout)) != 0 ||
        ::setsockopt(socket.get(), SOL_SOCKET, SO_SNDTIMEO, &timeout,
                     sizeof(timeout)) != 0) {
      continue;
    }
    return socket;
  }
  return Socket{};
}

bool send_all(const int descriptor, const std::span<const std::byte> bytes,
              net::Metrics& metrics) {
  std::size_t offset = 0;
  while (offset < bytes.size()) {
    const auto result = ::send(descriptor, bytes.data() + offset,
                               bytes.size() - offset, MSG_NOSIGNAL);
    if (result > 0) {
      const auto count = static_cast<std::size_t>(result);
      offset += count;
      metrics.add_bytes_written(count);
    } else if (result < 0 && errno == EINTR) {
      continue;
    } else {
      return false;
    }
  }
  return true;
}

std::optional<protocol::Frame> receive_one_frame(const int descriptor,
                                                 net::Metrics& metrics) {
  protocol::Parser parser;
  std::array<std::byte, 64U * 1024U> buffer{};
  while (true) {
    const auto result = ::recv(descriptor, buffer.data(), buffer.size(), 0);
    if (result > 0) {
      metrics.add_bytes_read(static_cast<std::size_t>(result));
      auto batch = parser.consume(std::span<const std::byte>{buffer}.first(
          static_cast<std::size_t>(result)));
      if (!batch.ok() || batch.frames.size() > 1U) {
        return std::nullopt;
      }
      if (!batch.frames.empty()) {
        return std::move(batch.frames.front());
      }
    } else if (result < 0 && errno == EINTR) {
      continue;
    } else {
      return std::nullopt;
    }
  }
}

bool is_request_message(const raft::Message& message) {
  return std::holds_alternative<raft::RequestVote>(message) ||
         std::holds_alternative<raft::AppendEntries>(message) ||
         std::holds_alternative<raft::InstallSnapshot>(message);
}

bool matches_peer_request(const raft::Message& request,
                          const raft::Message& response) {
  if (std::holds_alternative<raft::RequestVote>(request)) {
    return std::holds_alternative<raft::RequestVoteResponse>(response);
  }
  if (const auto* snapshot = std::get_if<raft::InstallSnapshot>(&request)) {
    const auto* reply = std::get_if<raft::InstallSnapshotResponse>(&response);
    return reply != nullptr && reply->rpc_id == snapshot->rpc_id &&
           reply->last_included_index == snapshot->last_included_index;
  }
  const auto* append = std::get_if<raft::AppendEntries>(&request);
  const auto* reply = std::get_if<raft::AppendEntriesResponse>(&response);
  if (append == nullptr || reply == nullptr || reply->rpc_id != append->rpc_id) {
    return false;
  }
  const auto last_sent =
      append->previous_log_index +
      static_cast<raft::LogIndex>(append->entries.size());
  return !reply->success || reply->match_index <= last_sent;
}

std::size_t peer_work_bytes(const raft::SendMessage& work) {
  return std::visit(
      [](const auto& message) {
        using Type = std::decay_t<decltype(message)>;
        std::size_t size = sizeof(Type);
        if constexpr (std::is_same_v<Type, raft::AppendEntries>) {
          for (const auto& entry : message.entries) {
            size += sizeof(raft::LogEntry) + entry.command.size();
          }
        } else if constexpr (std::is_same_v<Type, raft::InstallSnapshot>) {
          size += message.data.size();
        }
        return size;
      },
      work.message);
}

net::MetricsSnapshot combine_network_metrics(const net::MetricsSnapshot& left,
                                             const net::MetricsSnapshot& right) {
  return net::MetricsSnapshot{
      .active_connections = left.active_connections + right.active_connections,
      .accepted_connections_total =
          left.accepted_connections_total + right.accepted_connections_total,
      .closed_connections_total =
          left.closed_connections_total + right.closed_connections_total,
      .bytes_read_total = left.bytes_read_total + right.bytes_read_total,
      .bytes_written_total =
          left.bytes_written_total + right.bytes_written_total,
      .requests_in_flight = left.requests_in_flight + right.requests_in_flight,
      .read_buffer_bytes = left.read_buffer_bytes + right.read_buffer_bytes,
      .write_buffer_bytes = left.write_buffer_bytes + right.write_buffer_bytes,
      .connections_backpressured =
          left.connections_backpressured + right.connections_backpressured,
      .backpressure_events_total =
          left.backpressure_events_total + right.backpressure_events_total,
      .rejected_requests_total =
          left.rejected_requests_total + right.rejected_requests_total,
  };
}

std::vector<raft::NodeId> voter_ids(const ClusterNodeConfig& config) {
  std::vector<raft::NodeId> voters;
  voters.reserve(config.peers.size());
  for (const auto& peer : config.peers) {
    voters.push_back(peer.node_id);
  }
  return voters;
}

std::string_view role_text(const raft::Role role) {
  switch (role) {
    case raft::Role::follower:
      return "follower";
    case raft::Role::candidate:
      return "candidate";
    case raft::Role::leader:
      return "leader";
  }
  return "unknown";
}

enum class ClientStatus {
  ok,
  not_found,
  redirect,
  retry,
  failed,
  request_id_reuse,
  stale_request,
  capacity_exceeded,
};

struct ClientResult final {
  ClientStatus status{ClientStatus::failed};
  std::vector<std::byte> value;
  std::string leader_endpoint;
};

class PeerTransport final {
 public:
  using ResponseSink = std::function<void(raft::NodeId, raft::Message)>;

  PeerTransport(const ClusterNodeConfig& config, ResponseSink sink)
      : cluster_id_(config.cluster_id),
        self_id_(config.node_id),
        capacity_(config.peer_queue_capacity),
        byte_capacity_(config.peer_queue_byte_capacity),
        timeout_ms_(config.rpc_timeout_ms),
        worker_count_(config.peer_worker_threads),
        response_sink_(std::move(sink)) {
    for (const auto& peer : config.peers) {
      peers_.emplace(peer.node_id, peer);
    }
  }

  ~PeerTransport() { stop(); }

  void start() {
    for (std::size_t index = 0; index < worker_count_; ++index) {
      workers_.emplace_back([this] { worker_loop(); });
    }
  }

  bool send(raft::SendMessage message) {
    if (!is_request_message(message.message) ||
        !enabled_.load(std::memory_order_acquire)) {
      return false;
    }
    {
      const std::lock_guard lock(mutex_);
      if (stopping_ || !enabled_.load(std::memory_order_relaxed)) {
        return false;
      }
      const auto bytes = peer_work_bytes(message);
      const auto existing = std::ranges::find_if(
          queue_, [&message](const QueuedPeerWork& work) {
            return work.message.to == message.to;
          });
      if (existing != queue_.end()) {
        const auto remaining = queued_bytes_ - existing->bytes;
        if (bytes > byte_capacity_ - remaining) {
          metrics_.request_rejected();
          return false;
        }
        queued_bytes_ = remaining + bytes;
        *existing = QueuedPeerWork{.message = std::move(message),
                                   .bytes = bytes};
      } else {
        if (queue_.size() >= capacity_ || bytes > byte_capacity_ - queued_bytes_) {
          metrics_.request_rejected();
          return false;
        }
        queued_bytes_ += bytes;
        queue_.push_back(QueuedPeerWork{.message = std::move(message),
                                        .bytes = bytes});
      }
    }
    condition_.notify_one();
    return true;
  }

  void set_enabled(const bool enabled) {
    enabled_.store(enabled, std::memory_order_release);
    if (!enabled) {
      const std::lock_guard lock(mutex_);
      queue_.clear();
      queued_bytes_ = 0;
    }
    condition_.notify_all();
  }

  [[nodiscard]] bool enabled() const noexcept {
    return enabled_.load(std::memory_order_acquire);
  }

  [[nodiscard]] net::MetricsSnapshot metrics() const noexcept {
    return metrics_.snapshot();
  }

  void stop() {
    {
      const std::lock_guard lock(mutex_);
      if (stopping_) {
        return;
      }
      stopping_ = true;
      queue_.clear();
      queued_bytes_ = 0;
    }
    condition_.notify_all();
    for (auto& worker : workers_) {
      if (worker.joinable()) {
        worker.join();
      }
    }
    workers_.clear();
  }

 private:
  struct QueuedPeerWork final {
    raft::SendMessage message;
    std::size_t bytes{};
  };

  void worker_loop() {
    while (true) {
      raft::SendMessage work;
      {
        std::unique_lock lock(mutex_);
        condition_.wait(lock, [this] { return stopping_ || !queue_.empty(); });
        if (stopping_) {
          return;
        }
        queued_bytes_ -= queue_.front().bytes;
        work = std::move(queue_.front().message);
        queue_.pop_front();
      }
      const auto peer = peers_.find(work.to);
      if (peer == peers_.end() || !enabled()) {
        continue;
      }
      try {
        const auto request_id =
            next_request_id_.fetch_add(1, std::memory_order_relaxed);
        const auto frame = encode_peer_frame(
            PeerEnvelope{.cluster_id = cluster_id_,
                         .from = self_id_,
                         .to = work.to,
                         .message = work.message},
            request_id);
        const auto wire_frame = protocol::serialize(frame);
        if (!wire_frame.ok()) {
          continue;
        }
        auto socket = connect_with_timeout(peer->second.host,
                                           peer->second.peer_port, timeout_ms_);
        if (!socket.valid()) {
          continue;
        }
        metrics_.connection_opened();
        metrics_.request_started();
        struct Observation final {
          net::Metrics& metrics;
          ~Observation() {
            metrics.request_finished();
            metrics.connection_closed();
          }
        } observation{metrics_};
        if (!send_all(socket.get(), wire_frame.bytes, metrics_)) {
          continue;
        }
        auto response_frame = receive_one_frame(socket.get(), metrics_);
        if (!response_frame.has_value() ||
            response_frame->request_id != request_id) {
          continue;
        }
        auto response = decode_peer_frame(*response_frame);
        if (!response.ok() || response.value->cluster_id != cluster_id_ ||
            response.value->from != work.to ||
            response.value->to != self_id_ ||
            is_request_message(response.value->message) ||
            !matches_peer_request(work.message, response.value->message)) {
          continue;
        }
        if (enabled()) {
          response_sink_(work.to, std::move(response.value->message));
        }
      } catch (const std::exception&) {
        // A transport/encoding failure is a dropped Raft message. The Raft
        // timer and later heartbeats drive retry without blocking its owner.
      }
    }
  }

  std::uint64_t cluster_id_;
  raft::NodeId self_id_;
  std::size_t capacity_;
  std::size_t byte_capacity_;
  std::uint32_t timeout_ms_;
  std::size_t worker_count_;
  ResponseSink response_sink_;
  net::Metrics metrics_;
  std::unordered_map<raft::NodeId, PeerAddress> peers_;
  std::atomic<std::uint64_t> next_request_id_{1};
  std::atomic<bool> enabled_{true};
  std::mutex mutex_;
  std::condition_variable condition_;
  std::deque<QueuedPeerWork> queue_;
  std::size_t queued_bytes_{};
  std::vector<std::thread> workers_;
  bool stopping_{};
};

}  // namespace

PeerAddress parse_peer_address(const std::string_view text) {
  const auto invalid = []() -> void {
    throw std::invalid_argument(
        "peer must be node_id=host:peer_port:client_port or "
        "node_id=[ipv6]:peer_port:client_port");
  };
  const auto parse_number = [&](const std::string_view value,
                                const std::uint64_t maximum,
                                const bool allow_zero) {
    if (value.empty()) {
      invalid();
    }
    std::size_t consumed = 0;
    const auto number = std::stoull(std::string(value), &consumed);
    if (consumed != value.size() || number > maximum ||
        (!allow_zero && number == 0)) {
      invalid();
    }
    return number;
  };

  const auto equals = text.find('=');
  if (equals == std::string_view::npos || equals == 0 ||
      equals + 1U == text.size()) {
    invalid();
  }
  const auto node_id = parse_number(
      text.substr(0, equals), std::numeric_limits<std::uint64_t>::max(), true);
  const auto endpoint = text.substr(equals + 1U);

  std::string_view host;
  std::string_view ports;
  if (endpoint.front() == '[') {
    const auto closing = endpoint.find(']');
    if (closing == std::string_view::npos || closing == 1U ||
        closing + 1U >= endpoint.size() || endpoint[closing + 1U] != ':') {
      invalid();
    }
    host = endpoint.substr(1U, closing - 1U);
    ports = endpoint.substr(closing + 2U);
  } else {
    const auto first_colon = endpoint.find(':');
    if (first_colon == std::string_view::npos || first_colon == 0) {
      invalid();
    }
    host = endpoint.substr(0, first_colon);
    ports = endpoint.substr(first_colon + 1U);
  }

  const auto separator = ports.find(':');
  if (host.empty() || separator == std::string_view::npos || separator == 0 ||
      separator + 1U == ports.size() ||
      ports.find(':', separator + 1U) != std::string_view::npos) {
    invalid();
  }
  const auto peer_port = parse_number(
      ports.substr(0, separator),
      std::numeric_limits<std::uint16_t>::max(), false);
  const auto client_port = parse_number(
      ports.substr(separator + 1U),
      std::numeric_limits<std::uint16_t>::max(), false);
  return PeerAddress{.node_id = node_id,
                     .host = std::string(host),
                     .peer_port = static_cast<std::uint16_t>(peer_port),
                     .client_port = static_cast<std::uint16_t>(client_port)};
}

std::string format_endpoint(const std::string_view host,
                            const std::uint16_t port) {
  if (host.find(':') != std::string_view::npos) {
    return "[" + std::string(host) + "]:" + std::to_string(port);
  }
  return std::string(host) + ":" + std::to_string(port);
}

class ClusterNode::Impl final {
 public:
  explicit Impl(ClusterNodeConfig config)
      : config_(std::move(config)),
        metrics_(voter_ids(config_)),
        transport_(config_, [this](const raft::NodeId from,
                                   raft::Message response) {
          static_cast<void>(post(PeerResponse{.from = from,
                                              .message = std::move(response)}));
        }) {
    raft_metrics_observation_.peer_match.reserve(config_.peers.size() + 1U);
  }

  ~Impl() { stop(); }

  std::optional<std::string> start() {
    if (const auto error = validation_error(); error.has_value()) {
      return error;
    }
    {
      const std::lock_guard lock(lifecycle_mutex_);
      if (started_) {
        return std::string("ClusterNode instances can be started only once");
      }
      started_ = true;
    }

    transport_.start();
    owner_thread_ = std::thread([this] { owner_loop(); });
    {
      std::unique_lock lock(startup_mutex_);
      startup_condition_.wait(lock, [this] { return owner_ready_; });
      if (owner_error_.has_value()) {
        stop();
        return owner_error_;
      }
    }

    peer_server_ = std::make_unique<server::TcpServer>(
        net::ReactorConfig{},
        [this](const protocol::Frame& request) { return handle_peer(request); });
    if (const auto error =
            peer_server_->start(config_.bind_address, config_.peer_port);
        error.has_value()) {
      stop();
      return "peer listener: " + *error;
    }
    client_server_ = std::make_unique<server::TcpServer>(
        net::ReactorConfig{}, [this](const protocol::Frame& request) {
          return handle_client(request);
        }, [this](const server::RequestMetadata& request) {
          metrics_.request_rejected(request_operation(request.message_type));
        }, [this](const server::RequestMetadata&,
                  const server::ResponseMetadata& response) {
          metrics_.request_outcome(request_outcome(response));
        });
    if (const auto error =
            client_server_->start(config_.bind_address, config_.client_port);
        error.has_value()) {
      stop();
      return "client listener: " + *error;
    }
    admin_server_ = std::make_unique<server::AdminServer>(
        [this](const std::string_view path) { return handle_admin(path); });
    if (const auto error = admin_server_->start(config_.admin_bind_address,
                                                config_.admin_port);
        error.has_value()) {
      stop();
      return "admin listener: " + *error;
    }
    FORGEKV_LOG(common::Severity::info,
                "cluster_node_started node_id=" +
                    std::to_string(config_.node_id));
    return std::nullopt;
  }

  void stop() {
    stopping_atomic_.store(true, std::memory_order_release);
    if (admin_server_) {
      admin_server_->stop();
    }
    std::deque<QueuedWork> cancelled;
    {
      const std::lock_guard lock(queue_mutex_);
      stopping_ = true;
      cancelled.swap(queue_);
    }
    metrics_.set_queue_depth(0);
    for (auto& work : cancelled) {
      cancel(work.work);
    }
    queue_condition_.notify_all();
    if (owner_thread_.joinable()) {
      owner_thread_.join();
    }
    if (client_server_) {
      client_server_->stop();
    }
    if (peer_server_) {
      peer_server_->stop();
    }
    transport_.stop();
  }

  std::uint16_t client_port() const noexcept {
    return client_server_ ? client_server_->bound_port() : 0;
  }

  std::uint16_t peer_port() const noexcept {
    return peer_server_ ? peer_server_->bound_port() : 0;
  }

  std::uint16_t admin_port() const noexcept {
    return admin_server_ ? admin_server_->bound_port() : 0;
  }

  bool failed() const noexcept {
    return failed_.load(std::memory_order_acquire);
  }

  bool healthy() const noexcept {
    return owner_available_.load(std::memory_order_acquire) && !failed() &&
           !stopping_atomic_.load(std::memory_order_acquire);
  }

  bool ready() const noexcept {
    return healthy() && peer_traffic_enabled_.load(std::memory_order_acquire) &&
           published_role_.load(std::memory_order_acquire) == raft::Role::leader;
  }

  void set_peer_traffic_enabled(const bool enabled) {
    peer_traffic_enabled_.store(enabled, std::memory_order_release);
    transport_.set_enabled(enabled);
  }

 private:
  struct Proposal final {
    ReplicatedCommand command;
    std::shared_ptr<std::promise<ClientResult>> completion;
  };
  struct Read final {
    std::string key;
    std::shared_ptr<std::promise<ClientResult>> completion;
    std::shared_ptr<std::atomic<bool>> canceled;
    std::chrono::steady_clock::time_point deadline;
  };
  struct PeerRequest final {
    PeerEnvelope envelope;
    std::shared_ptr<std::promise<raft::Message>> completion;
  };
  struct PeerResponse final {
    raft::NodeId from{};
    raft::Message message;
  };
  using Work = std::variant<Proposal, Read, PeerRequest, PeerResponse>;
  struct QueuedWork final {
    Work work;
    std::chrono::steady_clock::time_point enqueued;
  };

  struct PendingMutation final {
    std::vector<std::byte> command;
    std::shared_ptr<std::promise<ClientResult>> completion;
  };
  struct PendingRead final {
    std::string key;
    std::shared_ptr<std::promise<ClientResult>> completion;
    std::shared_ptr<std::atomic<bool>> canceled;
    std::chrono::steady_clock::time_point deadline;
  };

  static void cancel(Work& work) {
    std::visit(
        [](auto& typed) {
          using Type = std::decay_t<decltype(typed)>;
          if constexpr (std::is_same_v<Type, Proposal> ||
                        std::is_same_v<Type, Read>) {
            typed.completion->set_value(ClientResult{
                .status = ClientStatus::retry,
                .value = {},
                .leader_endpoint = {},
            });
          } else if constexpr (std::is_same_v<Type, PeerRequest>) {
            typed.completion->set_exception(std::make_exception_ptr(
                std::runtime_error("Raft node is stopping")));
          }
        },
        work);
  }

  std::optional<std::string> validation_error() const {
    if (config_.cluster_id == 0 || config_.node_id == 0 ||
        config_.data_directory.empty() || config_.raft_queue_capacity == 0 ||
        config_.peer_queue_capacity == 0 ||
        config_.peer_queue_byte_capacity == 0 ||
        config_.peer_worker_threads == 0 ||
        config_.max_pending_reads == 0 ||
        config_.snapshot_threshold == 0 ||
        config_.rpc_timeout_ms == 0 || config_.client_timeout_ms == 0) {
      return "invalid zero/empty cluster configuration";
    }
    std::vector<raft::NodeId> ids;
    for (const auto& peer : config_.peers) {
      in_addr ipv4{};
      in6_addr ipv6{};
      const bool numeric_host =
          ::inet_pton(AF_INET, peer.host.c_str(), &ipv4) == 1 ||
          ::inet_pton(AF_INET6, peer.host.c_str(), &ipv6) == 1;
      if (peer.node_id == 0 || peer.host.empty() || peer.peer_port == 0 ||
          peer.client_port == 0 || !numeric_host ||
          std::ranges::find(ids, peer.node_id) != ids.end()) {
        return "invalid or duplicate peer endpoint";
      }
      ids.push_back(peer.node_id);
    }
    if (ids.size() < 3U || ids.size() > 7U || ids.size() % 2U == 0U ||
        std::ranges::find(ids, config_.node_id) == ids.end()) {
      return "membership must be odd, at least three, and include self";
    }
    if (config_.heartbeat_interval >= config_.election_timeout_min ||
        config_.election_timeout_min > config_.election_timeout_max) {
      return "invalid Raft timer relationship";
    }
    return std::nullopt;
  }

  bool post(Work work) {
    {
      const std::lock_guard lock(queue_mutex_);
      if (stopping_ || queue_.size() >= config_.raft_queue_capacity) {
        metrics_.queue_rejected();
        return false;
      }
      queue_.push_back(QueuedWork{.work = std::move(work),
                                  .enqueued = std::chrono::steady_clock::now()});
      metrics_.set_queue_depth(queue_.size());
    }
    queue_condition_.notify_one();
    return true;
  }

  std::string leader_endpoint(const std::optional<raft::NodeId> leader) const {
    if (!leader.has_value()) {
      return {};
    }
    const auto peer = std::ranges::find_if(
        config_.peers,
        [leader](const PeerAddress& value) { return value.node_id == *leader; });
    if (peer == config_.peers.end()) {
      return {};
    }
    return format_endpoint(peer->host, peer->client_port);
  }

  protocol::Frame result_frame(const protocol::Frame& request,
                               ClientResult result) const {
    switch (result.status) {
      case ClientStatus::ok:
        return client_response(request, protocol::MessageType::ok,
                               std::move(result.value));
      case ClientStatus::not_found:
        return client_response(request, protocol::MessageType::not_found);
      case ClientStatus::redirect:
        return result.leader_endpoint.empty()
                   ? busy_response(request)
                   : redirect_response(request, result.leader_endpoint);
      case ClientStatus::retry:
        return busy_response(request);
      case ClientStatus::failed:
        return error_response(request, 2, "replicated operation failed");
      case ClientStatus::request_id_reuse:
        return error_response(request, 3,
                              "request ID reused with different command");
      case ClientStatus::stale_request:
        return error_response(request, 4, "stale request ID");
      case ClientStatus::capacity_exceeded:
        return error_response(request, 5,
                              "deduplication retention capacity reached");
    }
    return error_response(request, 2, "unknown operation result");
  }

  static RequestOperation request_operation(const protocol::Frame& request) {
    return request_operation(request.message_type);
  }

  static RequestOperation request_operation(
      const protocol::MessageType message_type) {
    switch (message_type) {
      case protocol::MessageType::put:
        return RequestOperation::put;
      case protocol::MessageType::get:
        return RequestOperation::get;
      case protocol::MessageType::delete_key:
        return RequestOperation::delete_key;
      default:
        return RequestOperation::ping;
    }
  }

  static RequestOutcome request_outcome(const protocol::Frame& response) {
    switch (response.message_type) {
      case protocol::MessageType::ok:
        return RequestOutcome::ok;
      case protocol::MessageType::not_found:
        return RequestOutcome::not_found;
      case protocol::MessageType::redirect:
        return RequestOutcome::redirect;
      case protocol::MessageType::busy:
        return RequestOutcome::busy;
      case protocol::MessageType::error: {
        if (response.payload.size() < 2U) {
          return RequestOutcome::internal;
        }
        const auto code = static_cast<std::uint16_t>(
            (std::to_integer<std::uint16_t>(response.payload[0]) << 8U) |
            std::to_integer<std::uint16_t>(response.payload[1]));
        switch (code) {
          case 1:
            return RequestOutcome::invalid;
          case 3:
            return RequestOutcome::request_id_reuse;
          case 4:
            return RequestOutcome::stale_request;
          case 5:
            return RequestOutcome::capacity_exceeded;
          default:
            return RequestOutcome::internal;
        }
      }
      default:
        return RequestOutcome::internal;
    }
  }

  static RequestOutcome request_outcome(
      const server::ResponseMetadata& response) {
    switch (response.message_type) {
      case protocol::MessageType::ok:
        return RequestOutcome::ok;
      case protocol::MessageType::not_found:
        return RequestOutcome::not_found;
      case protocol::MessageType::redirect:
        return RequestOutcome::redirect;
      case protocol::MessageType::busy:
        return RequestOutcome::busy;
      case protocol::MessageType::error:
        switch (response.error_code.value_or(0U)) {
          case 1:
            return RequestOutcome::invalid;
          case 3:
            return RequestOutcome::request_id_reuse;
          case 4:
            return RequestOutcome::stale_request;
          case 5:
            return RequestOutcome::capacity_exceeded;
          default:
            return RequestOutcome::internal;
        }
      default:
        return RequestOutcome::internal;
    }
  }

  protocol::Frame handle_client(const protocol::Frame& request) {
    const auto operation = request_operation(request);
    const auto started = std::chrono::steady_clock::now();
    metrics_.request_started(operation);
    try {
      auto response = handle_client_impl(request);
      const auto outcome = request_outcome(response);
      metrics_.request_finished(
          operation,
          std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::steady_clock::now() - started));
      if (outcome == RequestOutcome::internal) {
        common::Logger::write(common::Severity::error,
                              "client_request_internal_error",
                              request.request_id);
      }
      return response;
    } catch (...) {
      metrics_.request_finished(
          operation,
          std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::steady_clock::now() - started));
      common::Logger::write(common::Severity::error,
                            "client_request_exception", request.request_id);
      throw;
    }
  }

  protocol::Frame handle_client_impl(const protocol::Frame& request) {
    if (request.message_namespace != protocol::Namespace::client) {
      return error_response(request, 1, "client port requires client namespace");
    }
    if (request.message_type == protocol::MessageType::ping) {
      return client_response(request, protocol::MessageType::ok,
                             request.payload);
    }

    auto completion = std::make_shared<std::promise<ClientResult>>();
    auto future = completion->get_future();
    std::shared_ptr<std::atomic<bool>> cancellation;
    if (request.message_type == protocol::MessageType::put ||
        request.message_type == protocol::MessageType::delete_key) {
      auto decoded = decode_client_mutation(request);
      if (!decoded.ok()) {
        return error_response(request, 1, decoded.error);
      }
      if (!post(Proposal{.command = std::move(*decoded.value),
                         .completion = completion})) {
        return busy_response(request);
      }
    } else if (request.message_type == protocol::MessageType::get) {
      auto decoded = decode_client_get(request);
      if (!decoded.ok()) {
        return error_response(request, 1, decoded.error);
      }
      cancellation = std::make_shared<std::atomic<bool>>(false);
      const auto deadline = std::chrono::steady_clock::now() +
                            std::chrono::milliseconds(
                                config_.client_timeout_ms);
      if (!post(Read{.key = std::move(*decoded.value),
                     .completion = completion,
                     .canceled = cancellation,
                     .deadline = deadline})) {
        return busy_response(request);
      }
    } else {
      return error_response(request, 1, "unsupported client request type");
    }

    if (future.wait_for(std::chrono::milliseconds(config_.client_timeout_ms)) !=
        std::future_status::ready) {
      if (cancellation) {
        cancellation->store(true, std::memory_order_release);
      }
      return busy_response(request);
    }
    return result_frame(request, future.get());
  }

  protocol::Frame handle_peer(const protocol::Frame& request) {
    if (!transport_.enabled()) {
      throw std::runtime_error("peer traffic is administratively disabled");
    }
    auto decoded = decode_peer_frame(request);
    const auto known_source =
        decoded.ok() && decoded.value->from != config_.node_id &&
        std::ranges::any_of(config_.peers, [&](const PeerAddress& peer) {
          return peer.node_id == decoded.value->from;
        });
    if (!decoded.ok() || decoded.value->cluster_id != config_.cluster_id ||
        decoded.value->to != config_.node_id ||
        !known_source || !is_request_message(decoded.value->message)) {
      throw std::invalid_argument("invalid peer request: " + decoded.error);
    }
    if (std::holds_alternative<raft::AppendEntries>(decoded.value->message)) {
      metrics_.append_entries();
    }
    auto completion = std::make_shared<std::promise<raft::Message>>();
    auto future = completion->get_future();
    const auto source = decoded.value->from;
    if (!post(PeerRequest{.envelope = std::move(*decoded.value),
                          .completion = completion}) ||
        future.wait_for(std::chrono::milliseconds(config_.rpc_timeout_ms * 4U)) !=
            std::future_status::ready) {
      throw std::runtime_error("Raft owner did not answer peer request");
    }
    return encode_peer_frame(PeerEnvelope{.cluster_id = config_.cluster_id,
                                          .from = config_.node_id,
                                          .to = source,
                                          .message = future.get()},
                             request.request_id);
  }

  void owner_loop() {
    try {
      auto voters = voter_ids(config_);
      const auto recovery_started = std::chrono::steady_clock::now();
      node_ = std::make_unique<raft::PersistedRaftNode>(
          raft::PersistedRaftNode::open(raft::PersistedRaftOptions{
              .config = raft::RaftConfig{
                  .self_id = config_.node_id,
                  .cluster_id = config_.cluster_id,
                  .voters = std::move(voters),
                  .election_timeout_min = config_.election_timeout_min,
                  .election_timeout_max = config_.election_timeout_max,
                  .heartbeat_interval = config_.heartbeat_interval,
                  .random_seed = config_.cluster_id ^ (config_.node_id << 32U),
              },
              .data_directory = config_.data_directory,
              .initial_time = 0,
              .output = [this](const raft::Action& action) { on_action(action); },
              .crash_hook = {},
              .sync_observer = [this](const std::chrono::microseconds duration) {
                metrics_.observe_sync(duration);
              },
          }));
      metrics_.recovery_finished(
          std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::steady_clock::now() - recovery_started));
      const auto snapshot = node_->snapshot();
      if (snapshot.durable_snapshot.has_value()) {
        auto decoded = decode_snapshot_state(
            snapshot.durable_snapshot->state_machine);
        if (!decoded.ok()) {
          throw std::runtime_error("invalid durable state-machine snapshot: " +
                                   decoded.error);
        }
        state_machine_.restore(std::move(*decoded.value));
      }
      role_ = snapshot.role;
      published_role_.store(snapshot.role, std::memory_order_release);
      leader_id_ = snapshot.leader_id;
      last_snapshot_index_ = snapshot.durable_snapshot.has_value()
                                 ? snapshot.durable_snapshot->last_included_index
                                 : 0;
      publish_raft_metrics(node_->status());
    } catch (const std::exception& error) {
      {
        const std::lock_guard lock(startup_mutex_);
        owner_error_ = error.what();
        owner_ready_ = true;
      }
      startup_condition_.notify_all();
      return;
    }
    {
      const std::lock_guard lock(startup_mutex_);
      owner_ready_ = true;
    }
    owner_available_.store(true, std::memory_order_release);
    startup_condition_.notify_all();

    const auto started = std::chrono::steady_clock::now();
    auto next_tick = started;
    while (true) {
      std::optional<QueuedWork> work;
      {
        std::unique_lock lock(queue_mutex_);
        queue_condition_.wait_until(lock, next_tick, [this] {
          return stopping_ || !queue_.empty();
        });
        if (stopping_) {
          break;
        }
        if (!queue_.empty()) {
          work.emplace(std::move(queue_.front()));
          queue_.pop_front();
          metrics_.set_queue_depth(queue_.size());
        }
      }
      try {
        const auto now = std::chrono::steady_clock::now();
        if (now >= next_tick) {
          const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                                   now - started)
                                   .count();
          node_->advance_time(static_cast<raft::LogicalTime>(elapsed));
          next_tick = now + 10ms;
        }
        expire_pending_reads(now);
        if (work.has_value()) {
          metrics_.observe_queueing(
              std::chrono::duration_cast<std::chrono::microseconds>(
                  now - work->enqueued));
          std::visit([this](auto& typed) { process(typed); }, work->work);
        }
        poll_snapshot_creation();
        publish_raft_metrics(node_->status());
      } catch (const raft::RaftStorageError& error) {
        common::Logger::write(common::Severity::critical,
                              std::string("raft_persistence_failed detail=") +
                                  error.what());
        fail_pending(ClientStatus::failed);
        failed_.store(true, std::memory_order_release);
        break;
      } catch (const std::exception& error) {
        common::Logger::write(common::Severity::critical,
                              std::string("raft_owner_failed detail=") +
                                  error.what());
        fail_pending(ClientStatus::failed);
        failed_.store(true, std::memory_order_release);
        break;
      }
    }
    owner_available_.store(false, std::memory_order_release);
    fail_pending(ClientStatus::retry);
  }

  void process(Proposal& proposal) {
    const auto snapshot = node_->snapshot();
    if (snapshot.role != raft::Role::leader) {
      proposal.completion->set_value(ClientResult{
          .status = snapshot.leader_id.has_value() ? ClientStatus::redirect
                                                   : ClientStatus::retry,
          .value = {},
          .leader_endpoint = leader_endpoint(snapshot.leader_id),
      });
      return;
    }
    const auto index = snapshot.durable_snapshot.has_value()
                           ? snapshot.durable_snapshot->last_included_index +
                                 snapshot.log.size() + 1U
                           : static_cast<raft::LogIndex>(snapshot.log.size()) +
                                 1U;
    auto encoded = encode_replicated_command(proposal.command);
    pending_.emplace(index, PendingMutation{.command = encoded,
                                            .completion = proposal.completion});
    active_proposal_index_ = index;
    node_->propose(std::move(encoded));
    active_proposal_index_.reset();
  }

  void process(Read& read) {
    const auto now = std::chrono::steady_clock::now();
    if (read.canceled->load(std::memory_order_acquire) ||
        now >= read.deadline) {
      read.completion->set_value(ClientResult{.status = ClientStatus::retry,
                                              .value = {},
                                              .leader_endpoint = {}});
      return;
    }
    const auto snapshot = node_->snapshot();
    if (snapshot.role != raft::Role::leader) {
      read.completion->set_value(ClientResult{
          .status = snapshot.leader_id.has_value() ? ClientStatus::redirect
                                                   : ClientStatus::retry,
          .value = {},
          .leader_endpoint = leader_endpoint(snapshot.leader_id),
      });
      return;
    }

    if (pending_reads_.size() >= config_.max_pending_reads) {
      read.completion->set_value(ClientResult{.status = ClientStatus::retry,
                                              .value = {},
                                              .leader_endpoint = {}});
      return;
    }

    PendingRead pending{.key = std::move(read.key),
                        .completion = read.completion,
                        .canceled = read.canceled,
                        .deadline = read.deadline};
    const auto index = snapshot.durable_snapshot.has_value()
                           ? snapshot.durable_snapshot->last_included_index +
                                 snapshot.log.size() + 1U
                           : static_cast<raft::LogIndex>(snapshot.log.size()) +
                                 1U;
    pending_reads_.emplace(index, std::vector<PendingRead>{std::move(pending)});
    active_read_barrier_index_ = index;
    node_->read_barrier();
    active_read_barrier_index_.reset();
  }

  void process(PeerRequest& request) {
    active_peer_from_ = request.envelope.from;
    active_peer_response_.reset();
    node_->step(request.envelope.from, request.envelope.message);
    active_peer_from_.reset();
    if (!active_peer_response_.has_value()) {
      request.completion->set_exception(std::make_exception_ptr(
          std::invalid_argument("Raft request produced no response")));
      return;
    }
    request.completion->set_value(std::move(*active_peer_response_));
    active_peer_response_.reset();
  }

  void process(PeerResponse& response) {
    node_->step(response.from, response.message);
  }

  void on_action(const raft::Action& action) {
    if (const auto* send = std::get_if<raft::SendMessage>(&action)) {
      if (std::holds_alternative<raft::AppendEntries>(send->message)) {
        metrics_.append_entries();
      }
      if (!is_request_message(send->message) && active_peer_from_.has_value() &&
          send->to == *active_peer_from_) {
        active_peer_response_ = send->message;
      } else if (is_request_message(send->message)) {
        static_cast<void>(transport_.send(*send));
      }
      return;
    }
    if (const auto* role = std::get_if<raft::RoleChanged>(&action)) {
      const bool lost_leadership = role_ == raft::Role::leader &&
                                   role->to != raft::Role::leader;
      role_ = role->to;
      published_role_.store(role->to, std::memory_order_release);
      leader_id_ = role->leader_id;
      FORGEKV_LOG(common::Severity::info,
                  "raft_role_changed node_id=" +
                      std::to_string(config_.node_id) + " from=" +
                      std::string(role_text(role->from)) + " to=" +
                      std::string(role_text(role->to)));
      if ((role->to == raft::Role::leader) !=
          (role->from == raft::Role::leader)) {
        metrics_.leadership_changed();
      }
      if (lost_leadership) {
        fail_pending(ClientStatus::retry);
      }
      return;
    }
    if (const auto* apply = std::get_if<raft::ApplyEntry>(&action)) {
      apply_entry(apply->entry);
      maybe_start_snapshot(apply->entry.index, apply->entry.term);
      return;
    }
    if (const auto* apply = std::get_if<raft::ApplySnapshot>(&action)) {
      auto decoded = decode_snapshot_state(apply->snapshot.state_machine);
      if (!decoded.ok()) {
        throw std::runtime_error("installed invalid state-machine snapshot: " +
                                 decoded.error);
      }
      state_machine_.restore(std::move(*decoded.value));
      last_snapshot_index_ = apply->snapshot.last_included_index;
      fail_pending(ClientStatus::retry);
      return;
    }
    if (std::holds_alternative<raft::ProposalRejected>(action) &&
        active_proposal_index_.has_value()) {
      const auto pending = pending_.find(*active_proposal_index_);
      if (pending != pending_.end()) {
        pending->second.completion->set_value(ClientResult{
            .status = leader_id_.has_value() ? ClientStatus::redirect
                                             : ClientStatus::retry,
            .value = {},
            .leader_endpoint = leader_endpoint(leader_id_),
        });
        pending_.erase(pending);
      }
      return;
    }
    if (std::holds_alternative<raft::ProposalRejected>(action) &&
        active_read_barrier_index_.has_value()) {
      const auto pending = pending_reads_.find(*active_read_barrier_index_);
      if (pending != pending_reads_.end()) {
        for (auto& read : pending->second) {
          read.completion->set_value(ClientResult{
              .status = leader_id_.has_value() ? ClientStatus::redirect
                                               : ClientStatus::retry,
              .value = {},
              .leader_endpoint = leader_endpoint(leader_id_),
          });
        }
        pending_reads_.erase(pending);
      }
    }
  }

  void apply_entry(const raft::LogEntry& entry) {
    if (entry.kind == raft::EntryKind::no_op) {
      const auto pending = pending_reads_.find(entry.index);
      if (pending != pending_reads_.end()) {
        const auto now = std::chrono::steady_clock::now();
        for (auto& read : pending->second) {
          if (read.canceled->load(std::memory_order_acquire) ||
              now >= read.deadline) {
            read.completion->set_value(
                ClientResult{.status = ClientStatus::retry,
                             .value = {},
                             .leader_endpoint = {}});
            continue;
          }
          const auto* value = state_machine_.find(read.key);
          if (value == nullptr) {
            read.completion->set_value(
                ClientResult{.status = ClientStatus::not_found,
                             .value = {},
                             .leader_endpoint = {}});
          } else {
            read.completion->set_value(
                ClientResult{.status = ClientStatus::ok,
                             .value = *value,
                             .leader_endpoint = {}});
          }
        }
        pending_reads_.erase(pending);
      }
      return;
    }
    auto decoded = decode_replicated_command(entry.command);
    if (!decoded.ok()) {
      throw std::runtime_error("committed invalid state-machine command: " +
                               decoded.error);
    }
    const auto result = state_machine_.apply(*decoded.value, entry.command);
    const auto pending = pending_.find(entry.index);
    if (pending != pending_.end()) {
      if (pending->second.command != entry.command) {
        pending->second.completion->set_value(ClientResult{
            .status = ClientStatus::failed,
            .value = {},
            .leader_endpoint = {},
        });
        pending_.erase(pending);
        return;
      }
      ClientStatus status = ClientStatus::ok;
      switch (result.status) {
        case MutationApplyStatus::applied:
        case MutationApplyStatus::duplicate:
          break;
        case MutationApplyStatus::request_id_reuse:
          status = ClientStatus::request_id_reuse;
          break;
        case MutationApplyStatus::stale_request:
          status = ClientStatus::stale_request;
          break;
        case MutationApplyStatus::capacity_exceeded:
          status = ClientStatus::capacity_exceeded;
          break;
      }
      pending->second.completion->set_value(
          ClientResult{.status = status,
                       .value = result.response,
                       .leader_endpoint = {}});
      pending_.erase(pending);
    }
  }

  void maybe_start_snapshot(const raft::LogIndex index,
                            const raft::Term term) {
    if (snapshot_future_.has_value() || index <= last_snapshot_index_ ||
        index - last_snapshot_index_ < config_.snapshot_threshold) {
      return;
    }
    auto state_copy = state_machine_.snapshot();
    const auto directory = config_.data_directory;
    const auto cluster_id = config_.cluster_id;
    const auto node_id = config_.node_id;
    std::vector<raft::NodeId> voters;
    voters.reserve(config_.peers.size());
    for (const auto& peer : config_.peers) {
      voters.push_back(peer.node_id);
    }
    const auto membership = raft::fixed_membership_fingerprint(voters);
    auto* const metrics = &metrics_;
    snapshot_future_.emplace(std::async(
        std::launch::async,
        [directory, cluster_id, node_id, membership, index, term, metrics,
         state = std::move(state_copy)]() mutable {
          const auto started = std::chrono::steady_clock::now();
          raft::StateMachineSnapshot snapshot{
              .last_included_index = index,
              .last_included_term = term,
              .state_machine = encode_snapshot_state(state),
          };
          raft::SnapshotStore::write_atomic(directory, cluster_id, node_id,
                                            membership, snapshot);
          metrics->observe_snapshot(
              std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::steady_clock::now() - started));
          return snapshot;
        }));
  }

  void publish_raft_metrics(const raft::RaftStatus& status) {
    if (status.role == raft::Role::candidate &&
        status.current_term > last_election_term_) {
      last_election_term_ = status.current_term;
      metrics_.election_started();
    }
    auto& observation = raft_metrics_observation_;
    observation.role = status.role;
    observation.term = status.current_term;
    observation.leader_id = status.leader_id;
    observation.commit_index = status.commit_index;
    observation.last_applied = status.last_applied;
    observation.last_log_index = status.last_log_index;
    observation.retained_log_records = status.retained_log_records;
    observation.peer_match.clear();
    if (status.role == raft::Role::leader) {
      observation.peer_match.emplace_back(config_.node_id,
                                          status.last_log_index);
      for (const auto& peer : config_.peers) {
        if (peer.node_id == config_.node_id) {
          continue;
        }
        const auto match = node_->match_index(peer.node_id);
        if (match.has_value()) {
          observation.peer_match.emplace_back(peer.node_id,
                                              *match);
        }
      }
    }
    metrics_.set_raft(observation);
  }

  server::AdminResponse handle_admin(const std::string_view path) {
    if (path == "/health") {
      const bool value = healthy();
      return server::AdminResponse{
          .status = value ? 200 : 503,
          .content_type = "application/json",
          .body = value ? "{\"status\":\"healthy\"}\n"
                        : "{\"status\":\"unhealthy\"}\n"};
    }
    if (path == "/ready") {
      const bool value = ready();
      const auto role = role_text(published_role_.load(std::memory_order_acquire));
      return server::AdminResponse{
          .status = value ? 200 : 503,
          .content_type = "application/json",
          .body = "{\"status\":\"" +
                  std::string(value ? "ready" : "not_ready") +
                  "\",\"role\":\"" + std::string(role) + "\"}\n"};
    }
    if (path == "/metrics") {
      auto snapshot = metrics_.snapshot();
      std::error_code error;
      const auto bytes =
          std::filesystem::file_size(config_.data_directory / "raft-log.wal",
                                     error);
      if (!error) {
        snapshot.wal_bytes = bytes;
      }
      return server::AdminResponse{
          .status = 200,
          .content_type = "text/plain; version=0.0.4; charset=utf-8",
          .body = render_prometheus(
              snapshot, sample_process_metrics(),
              client_server_ ? client_server_->metrics() : net::MetricsSnapshot{},
              combine_network_metrics(
                  peer_server_ ? peer_server_->metrics()
                               : net::MetricsSnapshot{},
                  transport_.metrics()))};
    }
    return server::AdminResponse{.status = 404,
                                 .content_type = "text/plain; charset=utf-8",
                                 .body = "not found\n"};
  }

  void poll_snapshot_creation() {
    if (!snapshot_future_.has_value() ||
        snapshot_future_->wait_for(std::chrono::seconds(0)) !=
            std::future_status::ready) {
      return;
    }
    raft::StateMachineSnapshot completed;
    try {
      completed = snapshot_future_->get();
    } catch (const std::exception& error) {
      snapshot_future_.reset();
      common::Logger::write(common::Severity::critical,
                            std::string("snapshot_failed detail=") +
                                error.what());
      throw;
    }
    snapshot_future_.reset();
    const auto current = node_->snapshot();
    const auto current_base = current.durable_snapshot.has_value()
                                  ? current.durable_snapshot->last_included_index
                                  : raft::LogIndex{0};
    if (completed.last_included_index <= current_base) {
      return;
    }
    node_->compact(std::move(completed), true);
    last_snapshot_index_ = node_->snapshot().durable_snapshot->last_included_index;
    const auto after_compaction = node_->snapshot();
    if (after_compaction.last_applied > last_snapshot_index_) {
      const auto applied = std::ranges::find_if(
          after_compaction.log, [&after_compaction](const raft::LogEntry& entry) {
            return entry.index == after_compaction.last_applied;
          });
      if (applied != after_compaction.log.end()) {
        maybe_start_snapshot(applied->index, applied->term);
      }
    }
  }

  void fail_pending(const ClientStatus status) {
    for (auto& [index, pending] : pending_) {
      static_cast<void>(index);
      pending.completion->set_value(ClientResult{.status = status,
                                                 .value = {},
                                                 .leader_endpoint = {}});
    }
    pending_.clear();
    for (auto& [index, reads] : pending_reads_) {
      static_cast<void>(index);
      for (auto& read : reads) {
        read.completion->set_value(ClientResult{.status = status,
                                                .value = {},
                                                .leader_endpoint = {}});
      }
    }
    pending_reads_.clear();
  }

  void expire_pending_reads(
      const std::chrono::steady_clock::time_point now) {
    for (auto& [index, reads] : pending_reads_) {
      static_cast<void>(index);
      std::erase_if(reads, [now](PendingRead& read) {
        const bool expired =
            read.canceled->load(std::memory_order_acquire) ||
            now >= read.deadline;
        if (expired) {
          read.completion->set_value(ClientResult{
              .status = ClientStatus::retry,
              .value = {},
              .leader_endpoint = {},
          });
        }
        return expired;
      });
    }
  }

  ClusterNodeConfig config_;
  OperationalMetrics metrics_;
  PeerTransport transport_;
  std::unique_ptr<server::TcpServer> client_server_;
  std::unique_ptr<server::TcpServer> peer_server_;
  std::unique_ptr<server::AdminServer> admin_server_;
  std::unique_ptr<raft::PersistedRaftNode> node_;
  std::thread owner_thread_;

  std::mutex lifecycle_mutex_;
  bool started_{};

  std::mutex startup_mutex_;
  std::condition_variable startup_condition_;
  bool owner_ready_{};
  std::optional<std::string> owner_error_;

  std::mutex queue_mutex_;
  std::condition_variable queue_condition_;
  std::deque<QueuedWork> queue_;
  bool stopping_{};

  raft::Role role_{raft::Role::follower};
  std::optional<raft::NodeId> leader_id_;
  std::optional<raft::NodeId> active_peer_from_;
  std::optional<raft::Message> active_peer_response_;
  std::optional<raft::LogIndex> active_proposal_index_;
  std::optional<raft::LogIndex> active_read_barrier_index_;
  std::unordered_map<raft::LogIndex, PendingMutation> pending_;
  std::unordered_map<raft::LogIndex, std::vector<PendingRead>> pending_reads_;
  ReplicatedStateMachine state_machine_;
  std::optional<std::future<raft::StateMachineSnapshot>> snapshot_future_;
  raft::LogIndex last_snapshot_index_{};
  raft::Term last_election_term_{};
  RaftObservation raft_metrics_observation_;
  std::atomic<bool> failed_{};
  std::atomic<bool> owner_available_{};
  std::atomic<bool> stopping_atomic_{};
  std::atomic<bool> peer_traffic_enabled_{true};
  std::atomic<raft::Role> published_role_{raft::Role::follower};
};

ClusterNode::ClusterNode(ClusterNodeConfig config)
    : impl_(std::make_unique<Impl>(std::move(config))) {}
ClusterNode::~ClusterNode() = default;

std::optional<std::string> ClusterNode::start() { return impl_->start(); }
void ClusterNode::stop() { impl_->stop(); }
void ClusterNode::set_peer_traffic_enabled(const bool enabled) {
  impl_->set_peer_traffic_enabled(enabled);
}
std::uint16_t ClusterNode::client_port() const noexcept {
  return impl_->client_port();
}
std::uint16_t ClusterNode::peer_port() const noexcept {
  return impl_->peer_port();
}
std::uint16_t ClusterNode::admin_port() const noexcept {
  return impl_->admin_port();
}
bool ClusterNode::failed() const noexcept {
  return impl_->failed();
}
bool ClusterNode::healthy() const noexcept { return impl_->healthy(); }
bool ClusterNode::ready() const noexcept { return impl_->ready(); }

}  // namespace forgekv::cluster
