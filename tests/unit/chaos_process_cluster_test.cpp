#include "chaos/process_cluster.h"

#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <set>
#include <string>

#include <gtest/gtest.h>

namespace forgekv::chaos {
namespace {

class ClusterDirectory final {
 public:
  ClusterDirectory() {
    static std::atomic<std::uint64_t> sequence{0U};
    path_ = std::filesystem::temp_directory_path() /
            ("forgekv-chaos-process-" + std::to_string(::getpid()) + "-" +
             std::to_string(sequence.fetch_add(1U)));
    std::filesystem::create_directories(path_);
  }
  ~ClusterDirectory() { std::filesystem::remove_all(path_); }
  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

 private:
  std::filesystem::path path_;
};

ProcessClusterConfig test_config(const std::filesystem::path& root) {
  return ProcessClusterConfig{.node_count = 3U,
                              .cluster_id = 7711U,
                              .server_path = FORGEKV_SERVER_PATH,
                              .root_directory = root,
                              .shutdown_grace = std::chrono::seconds(2)};
}

TEST(ProcessClusterTest, BuildsUniqueDirectedProxyTopologyForEverySource) {
  ClusterDirectory directory;
  ProcessCluster cluster(test_config(directory.path()));
  ASSERT_TRUE(cluster.prepare().ok());

  std::set<std::uint16_t> ports;
  for (std::uint64_t node = 1U; node <= 3U; ++node) {
    EXPECT_TRUE(ports.insert(cluster.client_port(node)).second);
    EXPECT_TRUE(ports.insert(cluster.peer_port(node)).second);
    EXPECT_TRUE(ports.insert(cluster.admin_port(node)).second);
    for (std::uint64_t peer = 1U; peer <= 3U; ++peer) {
      if (node == peer) {
        continue;
      }
      EXPECT_TRUE(ports.insert(cluster.proxy_port(node, peer)).second);
    }
  }
  EXPECT_NE(cluster.proxy_port(1U, 2U), cluster.proxy_port(3U, 2U));
  const auto arguments = cluster.server_arguments(1U);
  EXPECT_NE(arguments.find("--peer 2=127.0.0.1:" +
                           std::to_string(cluster.proxy_port(1U, 2U))),
            std::string::npos);
}

TEST(ProcessClusterTest, DirectPeerModeBypassesFaultProxies) {
  ClusterDirectory directory;
  auto config = test_config(directory.path());
  config.enable_proxies = false;
  ProcessCluster cluster(std::move(config));
  ASSERT_TRUE(cluster.prepare().ok());
  EXPECT_EQ(cluster.proxy_port(1U, 2U), 0U);
  const auto arguments = cluster.server_arguments(1U);
  EXPECT_NE(arguments.find("2=127.0.0.1:" +
                           std::to_string(cluster.peer_port(2U)) + ":"),
            std::string::npos);
}

TEST(ProcessClusterTest, OwnsPauseKillRestartAndBoundedStop) {
  ClusterDirectory directory;
  ProcessCluster cluster(test_config(directory.path()));
  ASSERT_TRUE(cluster.prepare().ok());
  ASSERT_TRUE(cluster.start_all().ok());
  EXPECT_EQ(cluster.refresh().nodes.size(), 3U);
  EXPECT_TRUE(cluster.pause_node(2U).ok());
  EXPECT_EQ(cluster.state(2U), NodeState::paused);
  EXPECT_TRUE(cluster.resume_node(2U).ok());
  EXPECT_EQ(cluster.state(2U), NodeState::running);
  EXPECT_TRUE(cluster.kill_node(2U).ok());
  EXPECT_EQ(cluster.state(2U), NodeState::dead);
  EXPECT_TRUE(cluster.restart_node(2U).ok());
  EXPECT_EQ(cluster.state(2U), NodeState::running);
  ASSERT_TRUE(cluster.stop_all().ok());
  EXPECT_EQ(cluster.state(1U), NodeState::dead);
  EXPECT_EQ(cluster.state(2U), NodeState::dead);
  EXPECT_EQ(cluster.state(3U), NodeState::dead);
}

TEST(ProcessClusterTest, RejectsInvalidLifecycleTransitions) {
  ClusterDirectory directory;
  ProcessCluster cluster(test_config(directory.path()));
  EXPECT_FALSE(cluster.start_all().ok());
  ASSERT_TRUE(cluster.prepare().ok());
  ASSERT_TRUE(cluster.restart_node(1U).ok());
  EXPECT_FALSE(cluster.start_node(1U).ok());
  EXPECT_FALSE(cluster.resume_node(1U).ok());
  ASSERT_TRUE(cluster.pause_node(1U).ok());
  EXPECT_FALSE(cluster.restart_node(1U).ok());
}

}  // namespace
}  // namespace forgekv::chaos
