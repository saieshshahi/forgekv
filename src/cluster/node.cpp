#include "cluster/node.h"

#include "cluster/codecs.h"
#include "protocol/parser.h"
#include "protocol/serializer.h"
#include "protocol/wire.h"
#include "raft/persisted_raft_node.h"
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

bool send_all(const int descriptor, const std::span<const std::byte> bytes) {
  std::size_t offset = 0;
  while (offset < bytes.size()) {
    const auto result = ::send(descriptor, bytes.data() + offset,
                               bytes.size() - offset, MSG_NOSIGNAL);
    if (result > 0) {
      offset += static_cast<std::size_t>(result);
    } else if (result < 0 && errno == EINTR) {
      continue;
    } else {
      return false;
    }
  }
  return true;
}

std::optional<protocol::Frame> receive_one_frame(const int descriptor) {
  protocol::Parser parser;
  std::array<std::byte, 64U * 1024U> buffer{};
  while (true) {
    const auto result = ::recv(descriptor, buffer.data(), buffer.size(), 0);
    if (result > 0) {
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
         std::holds_alternative<raft::AppendEntries>(message);
}

bool matches_peer_request(const raft::Message& request,
                          const raft::Message& response) {
  if (std::holds_alternative<raft::RequestVote>(request)) {
    return std::holds_alternative<raft::RequestVoteResponse>(response);
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
        }
        return size;
      },
      work.message);
}

enum class ClientStatus { ok, not_found, redirect, retry, failed };

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
    if (!is_request_message(message.message)) {
      return false;
    }
    {
      const std::lock_guard lock(mutex_);
      if (stopping_) {
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
          return false;
        }
        queued_bytes_ = remaining + bytes;
        *existing = QueuedPeerWork{.message = std::move(message),
                                   .bytes = bytes};
      } else {
        if (queue_.size() >= capacity_ || bytes > byte_capacity_ - queued_bytes_) {
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
      if (peer == peers_.end()) {
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
        if (!socket.valid() || !send_all(socket.get(), wire_frame.bytes)) {
          continue;
        }
        auto response_frame = receive_one_frame(socket.get());
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
        response_sink_(work.to, std::move(response.value->message));
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
  std::unordered_map<raft::NodeId, PeerAddress> peers_;
  std::atomic<std::uint64_t> next_request_id_{1};
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
        transport_(config_, [this](const raft::NodeId from,
                                   raft::Message response) {
          static_cast<void>(post(PeerResponse{.from = from,
                                              .message = std::move(response)}));
        }) {}

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
        });
    if (const auto error =
            client_server_->start(config_.bind_address, config_.client_port);
        error.has_value()) {
      stop();
      return "client listener: " + *error;
    }
    return std::nullopt;
  }

  void stop() {
    std::deque<Work> cancelled;
    {
      const std::lock_guard lock(queue_mutex_);
      stopping_ = true;
      cancelled.swap(queue_);
    }
    for (auto& work : cancelled) {
      cancel(work);
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

  bool failed() const noexcept {
    return failed_.load(std::memory_order_acquire);
  }

 private:
  struct Proposal final {
    ReplicatedCommand command;
    std::shared_ptr<std::promise<ClientResult>> completion;
  };
  struct Read final {
    std::string key;
    std::shared_ptr<std::promise<ClientResult>> completion;
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

  struct PendingMutation final {
    KvOperation operation{KvOperation::put};
    std::vector<std::byte> command;
    std::shared_ptr<std::promise<ClientResult>> completion;
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
        return false;
      }
      queue_.push_back(std::move(work));
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
    }
    return error_response(request, 2, "unknown operation result");
  }

  protocol::Frame handle_client(const protocol::Frame& request) {
    if (request.message_namespace != protocol::Namespace::client) {
      return error_response(request, 1, "client port requires client namespace");
    }
    if (request.message_type == protocol::MessageType::ping) {
      return client_response(request, protocol::MessageType::ok,
                             request.payload);
    }

    auto completion = std::make_shared<std::promise<ClientResult>>();
    auto future = completion->get_future();
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
      if (!post(Read{.key = std::move(*decoded.value),
                     .completion = completion})) {
        return busy_response(request);
      }
    } else {
      return error_response(request, 1, "unsupported client request type");
    }

    if (future.wait_for(std::chrono::milliseconds(config_.client_timeout_ms)) !=
        std::future_status::ready) {
      return busy_response(request);
    }
    return result_frame(request, future.get());
  }

  protocol::Frame handle_peer(const protocol::Frame& request) {
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
      std::vector<raft::NodeId> voters;
      voters.reserve(config_.peers.size());
      for (const auto& peer : config_.peers) {
        voters.push_back(peer.node_id);
      }
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
          }));
      const auto snapshot = node_->snapshot();
      role_ = snapshot.role;
      leader_id_ = snapshot.leader_id;
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
    startup_condition_.notify_all();

    const auto started = std::chrono::steady_clock::now();
    auto next_tick = started;
    while (true) {
      std::optional<Work> work;
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
        if (work.has_value()) {
          std::visit([this](auto& typed) { process(typed); }, *work);
        }
      } catch (const std::exception&) {
        fail_pending(ClientStatus::failed);
        failed_.store(true, std::memory_order_release);
        break;
      }
    }
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
    const auto index = static_cast<raft::LogIndex>(snapshot.log.size()) + 1U;
    auto encoded = encode_replicated_command(proposal.command);
    pending_.emplace(index,
                     PendingMutation{.operation = proposal.command.operation,
                                     .command = encoded,
                                     .completion = proposal.completion});
    active_proposal_index_ = index;
    node_->propose(std::move(encoded));
    active_proposal_index_.reset();
  }

  void process(Read& read) {
    const auto value = state_machine_.find(read.key);
    if (value == state_machine_.end()) {
      read.completion->set_value(ClientResult{.status = ClientStatus::not_found,
                                               .value = {},
                                               .leader_endpoint = {}});
    } else {
      read.completion->set_value(
          ClientResult{.status = ClientStatus::ok,
                       .value = value->second,
                       .leader_endpoint = {}});
    }
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
      leader_id_ = role->leader_id;
      if (lost_leadership) {
        fail_pending(ClientStatus::retry);
      }
      return;
    }
    if (const auto* apply = std::get_if<raft::ApplyEntry>(&action)) {
      apply_entry(apply->entry);
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
    }
  }

  void apply_entry(const raft::LogEntry& entry) {
    if (entry.kind == raft::EntryKind::no_op) {
      return;
    }
    auto decoded = decode_replicated_command(entry.command);
    if (!decoded.ok()) {
      throw std::runtime_error("committed invalid state-machine command: " +
                               decoded.error);
    }
    bool existed = false;
    if (decoded.value->operation == KvOperation::put) {
      state_machine_[decoded.value->key] = decoded.value->value;
    } else {
      existed = state_machine_.erase(decoded.value->key) != 0;
    }
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
      std::vector<std::byte> result;
      if (pending->second.operation == KvOperation::delete_key) {
        result.push_back(existed ? std::byte{1} : std::byte{0});
      }
      pending->second.completion->set_value(
          ClientResult{.status = ClientStatus::ok,
                       .value = std::move(result),
                       .leader_endpoint = {}});
      pending_.erase(pending);
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
  }

  ClusterNodeConfig config_;
  PeerTransport transport_;
  std::unique_ptr<server::TcpServer> client_server_;
  std::unique_ptr<server::TcpServer> peer_server_;
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
  std::deque<Work> queue_;
  bool stopping_{};

  raft::Role role_{raft::Role::follower};
  std::optional<raft::NodeId> leader_id_;
  std::optional<raft::NodeId> active_peer_from_;
  std::optional<raft::Message> active_peer_response_;
  std::optional<raft::LogIndex> active_proposal_index_;
  std::unordered_map<raft::LogIndex, PendingMutation> pending_;
  std::unordered_map<std::string, std::vector<std::byte>> state_machine_;
  std::atomic<bool> failed_{};
};

ClusterNode::ClusterNode(ClusterNodeConfig config)
    : impl_(std::make_unique<Impl>(std::move(config))) {}
ClusterNode::~ClusterNode() = default;

std::optional<std::string> ClusterNode::start() { return impl_->start(); }
void ClusterNode::stop() { impl_->stop(); }
std::uint16_t ClusterNode::client_port() const noexcept {
  return impl_->client_port();
}
std::uint16_t ClusterNode::peer_port() const noexcept {
  return impl_->peer_port();
}
bool ClusterNode::failed() const noexcept {
  return impl_->failed();
}

}  // namespace forgekv::cluster
