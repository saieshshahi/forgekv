#include "cluster/codecs.h"
#include "protocol/parser.h"
#include "protocol/serializer.h"
#include "protocol/wire.h"
#include "raft/persisted_raft_node.h"
#include "raft/raft_storage.h"

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <future>
#include <memory>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace forgekv::cluster {
namespace {

using namespace std::chrono_literals;

class ClusterDirectory final {
 public:
  ClusterDirectory() {
    static std::atomic<std::uint64_t> sequence{0};
    path_ = std::filesystem::temp_directory_path() /
            ("forgekv-real-cluster-" + std::to_string(::getpid()) + "-" +
             std::to_string(sequence.fetch_add(1)));
    std::filesystem::create_directories(path_);
  }
  ~ClusterDirectory() { std::filesystem::remove_all(path_); }
  [[nodiscard]] const std::filesystem::path& path() const { return path_; }

 private:
  std::filesystem::path path_;
};

std::uint16_t reserve_port(std::set<std::uint16_t>& used) {
  while (true) {
    const int descriptor = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (descriptor < 0) {
      return 0;
    }
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (::bind(descriptor, reinterpret_cast<const sockaddr*>(&address),
               sizeof(address)) != 0) {
      ::close(descriptor);
      return 0;
    }
    socklen_t size = sizeof(address);
    if (::getsockname(descriptor, reinterpret_cast<sockaddr*>(&address),
                      &size) != 0) {
      ::close(descriptor);
      return 0;
    }
    const auto port = ntohs(address.sin_port);
    ::close(descriptor);
    if (port != 0 && used.insert(port).second) {
      return port;
    }
  }
}

class ServerProcess final {
 public:
  ServerProcess(std::vector<std::string> arguments,
                std::filesystem::path log_path)
      : arguments_(std::move(arguments)), log_path_(std::move(log_path)) {}
  ~ServerProcess() { stop(); }
  ServerProcess(const ServerProcess&) = delete;
  ServerProcess& operator=(const ServerProcess&) = delete;

  bool start() {
    if (pid_ > 0) {
      return false;
    }
    std::vector<char*> argv;
    argv.reserve(arguments_.size() + 2U);
    argv.push_back(const_cast<char*>(FORGEKV_SERVER_PATH));
    for (auto& argument : arguments_) {
      argv.push_back(argument.data());
    }
    argv.push_back(nullptr);
    pid_ = ::fork();
    if (pid_ == 0) {
      const auto log = ::open(log_path_.c_str(),
                              O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
      if (log >= 0) {
        static_cast<void>(::dup2(log, STDOUT_FILENO));
        static_cast<void>(::dup2(log, STDERR_FILENO));
      }
      ::execv(FORGEKV_SERVER_PATH, argv.data());
      ::_exit(126);
    }
    return pid_ > 0;
  }

  void kill9() {
    if (pid_ > 0) {
      static_cast<void>(::kill(pid_, SIGKILL));
      wait();
    }
  }

  void toggle_peer_partition() {
    if (pid_ > 0) {
      static_cast<void>(::kill(pid_, SIGUSR1));
      std::this_thread::sleep_for(250ms);
    }
  }

  void stop() {
    if (pid_ <= 0) {
      return;
    }
    static_cast<void>(::kill(pid_, SIGTERM));
    for (int attempt = 0; attempt < 100; ++attempt) {
      int status = 0;
      const auto result = ::waitpid(pid_, &status, WNOHANG);
      if (result == pid_) {
        pid_ = -1;
        return;
      }
      std::this_thread::sleep_for(10ms);
    }
    static_cast<void>(::kill(pid_, SIGKILL));
    wait();
  }

 private:
  void wait() {
    int status = 0;
    while (::waitpid(pid_, &status, 0) < 0 && errno == EINTR) {
    }
    pid_ = -1;
  }

  std::vector<std::string> arguments_;
  std::filesystem::path log_path_;
  pid_t pid_{-1};
};

class Socket final {
 public:
  explicit Socket(const int descriptor = -1) : descriptor_(descriptor) {}
  ~Socket() {
    if (descriptor_ >= 0) {
      ::close(descriptor_);
    }
  }
  Socket(const Socket&) = delete;
  Socket& operator=(const Socket&) = delete;
  Socket(Socket&& other) noexcept
      : descriptor_(std::exchange(other.descriptor_, -1)) {}
  [[nodiscard]] int get() const { return descriptor_; }
  [[nodiscard]] bool valid() const { return descriptor_ >= 0; }

 private:
  int descriptor_;
};

Socket connect_to(const std::uint16_t port) {
  Socket socket(::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0));
  if (!socket.valid()) {
    return Socket{};
  }
  timeval timeout{.tv_sec = 6, .tv_usec = 0};
  static_cast<void>(::setsockopt(socket.get(), SOL_SOCKET, SO_RCVTIMEO,
                                 &timeout, sizeof(timeout)));
  static_cast<void>(::setsockopt(socket.get(), SOL_SOCKET, SO_SNDTIMEO,
                                 &timeout, sizeof(timeout)));
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(port);
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  if (::connect(socket.get(), reinterpret_cast<const sockaddr*>(&address),
                sizeof(address)) != 0) {
    return Socket{};
  }
  return socket;
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

std::optional<protocol::Frame> request(const std::uint16_t port,
                                       const protocol::Frame& frame) {
  auto socket = connect_to(port);
  const auto encoded = protocol::serialize(frame);
  if (!socket.valid() || !encoded.ok() ||
      !send_all(socket.get(), encoded.bytes)) {
    return std::nullopt;
  }
  protocol::Parser parser;
  std::array<std::byte, 64U * 1024U> bytes{};
  while (true) {
    const auto count = ::recv(socket.get(), bytes.data(), bytes.size(), 0);
    if (count <= 0) {
      return std::nullopt;
    }
    auto batch = parser.consume(std::span<const std::byte>{bytes}.first(
        static_cast<std::size_t>(count)));
    if (!batch.ok()) {
      return std::nullopt;
    }
    if (!batch.frames.empty()) {
      return std::move(batch.frames.front());
    }
  }
}

void copy_text(const std::string& text,
               const std::span<std::byte> destination) {
  std::ranges::transform(text, destination.begin(), [](const char character) {
    return static_cast<std::byte>(static_cast<unsigned char>(character));
  });
}

protocol::Frame put(const std::uint64_t request_id, const std::string& key,
                    const std::string& value) {
  std::vector<std::byte> payload(24U + key.size() + value.size());
  for (std::size_t index = 0; index < 16U; ++index) {
    payload[index] = static_cast<std::byte>(index + 1U);
  }
  protocol::wire::write_u32(std::span{payload}.subspan(16, 4),
                            static_cast<std::uint32_t>(key.size()));
  protocol::wire::write_u32(std::span{payload}.subspan(20, 4),
                            static_cast<std::uint32_t>(value.size()));
  copy_text(key, std::span{payload}.subspan(24, key.size()));
  copy_text(value,
            std::span{payload}.subspan(24U + key.size(), value.size()));
  return protocol::Frame{.message_namespace = protocol::Namespace::client,
                         .message_type = protocol::MessageType::put,
                         .flags = 0,
                         .request_id = request_id,
                         .payload = std::move(payload)};
}

protocol::Frame get(const std::uint64_t request_id, const std::string& key) {
  std::vector<std::byte> payload(4U + key.size());
  protocol::wire::write_u32(std::span{payload}.first<4>(),
                            static_cast<std::uint32_t>(key.size()));
  copy_text(key, std::span{payload}.subspan(4));
  return protocol::Frame{.message_namespace = protocol::Namespace::client,
                         .message_type = protocol::MessageType::get,
                         .flags = 0,
                         .request_id = request_id,
                         .payload = std::move(payload)};
}

std::optional<std::size_t> find_leader(
    const std::array<std::uint16_t, 3>& client_ports,
    const std::set<std::size_t>& live_nodes, std::uint64_t& request_id) {
  const auto deadline = std::chrono::steady_clock::now() + 15s;
  std::size_t attempt = 0;
  while (std::chrono::steady_clock::now() < deadline) {
    for (const auto index : live_nodes) {
      const auto response = request(
          client_ports[index],
          put(++request_id, "__leader_probe", std::to_string(attempt)));
      if (response.has_value() &&
          response->message_type == protocol::MessageType::ok) {
        return index;
      }
    }
    ++attempt;
    std::this_thread::sleep_for(100ms);
  }
  return std::nullopt;
}

bool wait_for_value(const std::uint16_t port, const std::string& key,
                    const std::string& expected, std::uint64_t& request_id) {
  const auto deadline = std::chrono::steady_clock::now() + 15s;
  while (std::chrono::steady_clock::now() < deadline) {
    const auto response = request(port, get(++request_id, key));
    if (response.has_value() &&
        response->message_type == protocol::MessageType::ok) {
      const std::string value(reinterpret_cast<const char*>(response->payload.data()),
                              response->payload.size());
      if (value == expected) {
        return true;
      }
    }
    std::this_thread::sleep_for(100ms);
  }
  return false;
}

std::optional<std::string> wait_for_redirect(
    const std::uint16_t port, const std::string& expected,
    std::uint64_t& request_id) {
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (std::chrono::steady_clock::now() < deadline) {
    const auto response = request(port, put(++request_id, "redirect-probe", "x"));
    if (response.has_value() &&
        response->message_type == protocol::MessageType::redirect &&
        response->payload.size() >= 2U) {
      const auto encoded_size =
          (std::to_integer<std::uint16_t>(response->payload[0]) << 8U) |
          std::to_integer<std::uint16_t>(response->payload[1]);
      const auto size = static_cast<std::size_t>(encoded_size);
      if (response->payload.size() == 2U + size) {
        const std::string endpoint(
            reinterpret_cast<const char*>(response->payload.data() + 2U),
            size);
        if (endpoint == expected) {
          return endpoint;
        }
      }
    }
    std::this_thread::sleep_for(20ms);
  }
  return std::nullopt;
}

std::optional<std::string> wait_for_get_redirect(
    const std::uint16_t port, const std::string& expected,
    std::uint64_t& request_id) {
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (std::chrono::steady_clock::now() < deadline) {
    const auto response = request(port, get(++request_id, "alpha"));
    if (response.has_value() &&
        response->message_type == protocol::MessageType::redirect &&
        response->payload.size() >= 2U) {
      const auto encoded_size =
          (std::to_integer<std::uint16_t>(response->payload[0]) << 8U) |
          std::to_integer<std::uint16_t>(response->payload[1]);
      const auto size = static_cast<std::size_t>(encoded_size);
      if (response->payload.size() == 2U + size) {
        const std::string endpoint(
            reinterpret_cast<const char*>(response->payload.data() + 2U),
            size);
        if (endpoint == expected) {
          return endpoint;
        }
      }
    }
    std::this_thread::sleep_for(20ms);
  }
  return std::nullopt;
}

bool durable_has_put(const std::filesystem::path& directory,
                     const raft::NodeId node_id, const std::string& key,
                     const std::string& value) {
  auto storage = raft::RaftStorage::open(
      directory, 4242, node_id, {},
      raft::fixed_membership_fingerprint({1, 2, 3}));
  for (const auto& entry : storage.state().log) {
    if (entry.kind != raft::EntryKind::command) {
      continue;
    }
    const auto command = decode_replicated_command(entry.command);
    if (command.ok() && command.value->operation == KvOperation::put &&
        command.value->key == key) {
      const std::string stored(
          reinterpret_cast<const char*>(command.value->value.data()),
          command.value->value.size());
      if (stored == value) {
        return true;
      }
    }
  }
  return false;
}

TEST(RealCluster, ElectsFailsOverAndCatchesUpRestartedLeader) {
  ClusterDirectory directory;
  std::set<std::uint16_t> used_ports;
  std::array<std::uint16_t, 3> client_ports{};
  std::array<std::uint16_t, 3> peer_ports{};
  for (std::size_t index = 0; index < 3U; ++index) {
    client_ports[index] = reserve_port(used_ports);
    peer_ports[index] = reserve_port(used_ports);
    ASSERT_NE(client_ports[index], 0);
    ASSERT_NE(peer_ports[index], 0);
  }

  std::vector<std::string> peer_arguments;
  for (std::size_t index = 0; index < 3U; ++index) {
    peer_arguments.push_back(std::to_string(index + 1U) +
                             "=127.0.0.1:" +
                             std::to_string(peer_ports[index]) + ":" +
                             std::to_string(client_ports[index]));
  }
  std::vector<std::unique_ptr<ServerProcess>> processes;
  std::array<std::filesystem::path, 3> node_directories;
  for (std::size_t index = 0; index < 3U; ++index) {
    const auto node_directory = directory.path() /
                                ("node-" + std::to_string(index + 1U));
    node_directories[index] = node_directory;
    std::filesystem::create_directories(node_directory);
    std::vector<std::string> arguments{
        "--cluster-id", "4242", "--node-id", std::to_string(index + 1U),
        "--data-dir", node_directory.string(), "--client-port",
        std::to_string(client_ports[index]), "--peer-port",
        std::to_string(peer_ports[index]), "--client-timeout-ms", "3000",
        "--max-pending-reads", "1",
    };
    for (const auto& peer : peer_arguments) {
      arguments.push_back("--peer");
      arguments.push_back(peer);
    }
    processes.push_back(std::make_unique<ServerProcess>(
        std::move(arguments), directory.path() /
                                  ("node-" + std::to_string(index + 1U) +
                                   ".log")));
    ASSERT_TRUE(processes.back()->start());
  }

  std::uint64_t request_id = 100;
  const std::set<std::size_t> all_nodes{0, 1, 2};
  const auto first_leader = find_leader(client_ports, all_nodes, request_id);
  ASSERT_TRUE(first_leader.has_value());
  auto response = request(client_ports[*first_leader],
                          put(++request_id, "alpha", "one"));
  ASSERT_TRUE(response.has_value());
  ASSERT_EQ(response->message_type, protocol::MessageType::ok);
  ASSERT_TRUE(wait_for_value(client_ports[*first_leader], "alpha", "one",
                             request_id));

  const auto follower = (*first_leader + 1U) % client_ports.size();
  const auto leader_endpoint =
      "127.0.0.1:" + std::to_string(client_ports[*first_leader]);
  EXPECT_EQ(wait_for_redirect(client_ports[follower], leader_endpoint,
                              request_id),
            leader_endpoint);
  EXPECT_EQ(wait_for_get_redirect(client_ports[follower], leader_endpoint,
                                  request_id),
            leader_endpoint);

  const auto leader_node_id = static_cast<raft::NodeId>(*first_leader + 1U);
  auto hostile = encode_peer_frame(
      PeerEnvelope{.cluster_id = 4242,
                   .from = leader_node_id,
                   .to = leader_node_id,
                   .message = raft::RequestVote{
                       .term = 1,
                       .candidate_id = leader_node_id,
                       .last_log_index = 0,
                       .last_log_term = 0,
                   }},
      ++request_id);
  EXPECT_TRUE(request(peer_ports[*first_leader], hostile).has_value());
  hostile = encode_peer_frame(
      PeerEnvelope{.cluster_id = 4242,
                   .from = 99,
                   .to = leader_node_id,
                   .message = raft::RequestVote{.term = 1,
                                                .candidate_id = 99,
                                                .last_log_index = 0,
                                                .last_log_term = 0}},
      ++request_id);
  EXPECT_TRUE(request(peer_ports[*first_leader], hostile).has_value());
  const auto valid_peer_id = static_cast<raft::NodeId>(follower + 1U);
  hostile = encode_peer_frame(
      PeerEnvelope{
          .cluster_id = 4242,
          .from = valid_peer_id,
          .to = leader_node_id,
          .message = raft::AppendEntries{
              .term = 1,
              .leader_id = valid_peer_id,
              .previous_log_index = 0,
              .previous_log_term = 0,
              .entries = {raft::LogEntry{.index = 1,
                                         .term = 1,
                                         .kind = raft::EntryKind::command,
                                         .command = {std::byte{1}}}},
              .leader_commit = 0,
              .rpc_id = 1,
          }},
      ++request_id);
  hostile.payload[104] = static_cast<std::byte>(raft::EntryKind::no_op);
  EXPECT_TRUE(request(peer_ports[*first_leader], hostile).has_value());
  response = request(client_ports[*first_leader],
                     put(++request_id, "after-hostile", "alive"));
  ASSERT_TRUE(response.has_value());
  ASSERT_EQ(response->message_type, protocol::MessageType::ok);

  processes[follower]->kill9();
  const std::string large_value(600U * 1024U, 'z');
  for (const auto* key : {"large-one", "large-two"}) {
    response = request(client_ports[*first_leader],
                       put(++request_id, key, large_value));
    ASSERT_TRUE(response.has_value());
    ASSERT_EQ(response->message_type, protocol::MessageType::ok);
  }
  ASSERT_TRUE(processes[follower]->start());
  ASSERT_TRUE(wait_for_value(client_ports[*first_leader], "large-two",
                             large_value, request_id));

  processes[*first_leader]->toggle_peer_partition();
  auto survivors = all_nodes;
  survivors.erase(*first_leader);
  const auto second_leader = find_leader(client_ports, survivors, request_id);
  ASSERT_TRUE(second_leader.has_value());
  ASSERT_NE(*second_leader, *first_leader);
  response = request(client_ports[*second_leader],
                     put(++request_id, "beta", "two"));
  ASSERT_TRUE(response.has_value());
  ASSERT_EQ(response->message_type, protocol::MessageType::ok);
  ASSERT_TRUE(wait_for_value(client_ports[*second_leader], "beta", "two",
                             request_id));

  const auto first_stale_id = ++request_id;
  auto first_stale_read = std::async(std::launch::async, [&] {
    return request(client_ports[*first_leader], get(first_stale_id, "beta"));
  });
  std::this_thread::sleep_for(250ms);
  const auto rejected_at = std::chrono::steady_clock::now();
  response = request(client_ports[*first_leader], get(++request_id, "alpha"));
  const auto rejection_latency =
      std::chrono::steady_clock::now() - rejected_at;
  ASSERT_TRUE(response.has_value());
  EXPECT_NE(response->message_type, protocol::MessageType::ok);
  EXPECT_LT(rejection_latency, 2s);
  response = first_stale_read.get();
  ASSERT_TRUE(response.has_value());
  EXPECT_NE(response->message_type, protocol::MessageType::ok);
  const auto post_timeout_at = std::chrono::steady_clock::now();
  response = request(client_ports[*first_leader], get(++request_id, "beta"));
  const auto post_timeout_latency =
      std::chrono::steady_clock::now() - post_timeout_at;
  ASSERT_TRUE(response.has_value());
  EXPECT_NE(response->message_type, protocol::MessageType::ok);
  EXPECT_LT(post_timeout_latency, 2s);

  processes[*first_leader]->toggle_peer_partition();
  bool caught_up = false;
  for (int attempt = 0; attempt < 20 && !caught_up; ++attempt) {
    std::this_thread::sleep_for(200ms);
    processes[*first_leader]->kill9();
    caught_up = durable_has_put(node_directories[*first_leader],
                                static_cast<raft::NodeId>(*first_leader + 1U),
                                "beta", "two");
    if (!caught_up) {
      ASSERT_TRUE(processes[*first_leader]->start());
    }
  }
  EXPECT_TRUE(caught_up);
}

}  // namespace
}  // namespace forgekv::cluster
