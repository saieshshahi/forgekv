#pragma once

#include "raft/types.h"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace forgekv::cluster {

struct PeerAddress final {
  raft::NodeId node_id{};
  std::string host;
  std::uint16_t peer_port{};
  std::uint16_t client_port{};

  bool operator==(const PeerAddress&) const = default;
};

[[nodiscard]] PeerAddress parse_peer_address(std::string_view text);
[[nodiscard]] std::string format_endpoint(std::string_view host,
                                          std::uint16_t port);

struct ClusterNodeConfig final {
  std::uint64_t cluster_id{};
  raft::NodeId node_id{};
  std::filesystem::path data_directory;
  std::string bind_address{"127.0.0.1"};
  std::uint16_t client_port{};
  std::uint16_t peer_port{};
  std::vector<PeerAddress> peers;
  raft::LogicalTime election_timeout_min{600};
  raft::LogicalTime election_timeout_max{1'000};
  raft::LogicalTime heartbeat_interval{150};
  std::size_t raft_queue_capacity{4096};
  std::size_t peer_queue_capacity{4096};
  std::size_t peer_queue_byte_capacity{16U * 1024U * 1024U};
  std::size_t peer_worker_threads{2};
  std::uint32_t rpc_timeout_ms{500};
  std::uint32_t client_timeout_ms{10'000};
};

class ClusterNode final {
 public:
  explicit ClusterNode(ClusterNodeConfig config);
  ~ClusterNode();

  ClusterNode(const ClusterNode&) = delete;
  ClusterNode& operator=(const ClusterNode&) = delete;

  [[nodiscard]] std::optional<std::string> start();
  void stop();

  [[nodiscard]] std::uint16_t client_port() const noexcept;
  [[nodiscard]] std::uint16_t peer_port() const noexcept;
  [[nodiscard]] bool failed() const noexcept;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace forgekv::cluster
