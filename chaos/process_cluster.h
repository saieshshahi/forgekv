#pragma once

#include "chaos/fault_proxy.h"
#include "chaos/types.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

namespace forgekv::chaos {

struct ProcessClusterConfig final {
  std::size_t node_count{3U};
  std::uint64_t cluster_id{1U};
  std::filesystem::path server_path;
  std::filesystem::path root_directory;
  std::chrono::milliseconds shutdown_grace{std::chrono::seconds(5)};
  std::uint64_t proxy_seed{1U};
  bool enable_proxies{true};
};

struct ClusterStatus final {
  std::string error;
  [[nodiscard]] bool ok() const noexcept { return error.empty(); }
};

class ProcessCluster final {
 public:
  explicit ProcessCluster(ProcessClusterConfig config);
  ~ProcessCluster();

  ProcessCluster(const ProcessCluster&) = delete;
  ProcessCluster& operator=(const ProcessCluster&) = delete;

  [[nodiscard]] ClusterStatus prepare();
  [[nodiscard]] ClusterStatus start_all();
  [[nodiscard]] ClusterStatus start_node(std::uint64_t node);
  [[nodiscard]] ClusterStatus restart_node(std::uint64_t node);
  [[nodiscard]] ClusterStatus kill_node(std::uint64_t node);
  [[nodiscard]] ClusterStatus pause_node(std::uint64_t node);
  [[nodiscard]] ClusterStatus resume_node(std::uint64_t node);
  [[nodiscard]] ClusterStatus stop_all() noexcept;

  [[nodiscard]] ClusterView refresh();
  [[nodiscard]] NodeState state(std::uint64_t node) const noexcept;
  [[nodiscard]] std::uint16_t client_port(std::uint64_t node) const noexcept;
  [[nodiscard]] std::uint16_t peer_port(std::uint64_t node) const noexcept;
  [[nodiscard]] std::uint16_t admin_port(std::uint64_t node) const noexcept;
  [[nodiscard]] std::int64_t process_id(std::uint64_t node) const noexcept;
  [[nodiscard]] std::uint16_t proxy_port(std::uint64_t source,
                                         std::uint64_t destination) const noexcept;
  [[nodiscard]] std::string server_arguments(std::uint64_t node) const;
  [[nodiscard]] ClusterStatus set_link_policy(std::uint64_t source,
                                               std::uint64_t destination,
                                               LinkPolicy policy);
  void heal_network() noexcept;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace forgekv::chaos
