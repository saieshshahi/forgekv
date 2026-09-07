#include "chaos/verifier.h"

#include <cstdint>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace forgekv::chaos {
namespace {

std::string metrics(const std::string& role, const std::uint64_t term,
                    const std::uint64_t leader, const std::uint64_t index,
                    const bool include_lag = false) {
  std::string text =
      "forgekv_process_rss_bytes 4096\n"
      "forgekv_raft_role{role=\"" +
      role + "\"} 1\n" + "forgekv_raft_term " + std::to_string(term) +
      "\nforgekv_raft_leader_id " + std::to_string(leader) +
      "\nforgekv_raft_commit_index " + std::to_string(index) +
      "\nforgekv_raft_last_applied " + std::to_string(index) + "\n";
  if (include_lag) {
    text += "forgekv_raft_replication_lag{peer=\"1\"} 0\n"
            "forgekv_raft_replication_lag{peer=\"2\"} 0\n"
            "forgekv_raft_replication_lag{peer=\"3\"} 0\n";
  }
  return text;
}

std::vector<NodeOperationalView> converged_views() {
  std::vector<NodeOperationalView> result;
  for (std::uint64_t node = 1U; node <= 3U; ++node) {
    const auto parsed = parse_node_view(
        node, true, node == 2U,
        metrics(node == 2U ? "leader" : "follower", 7U, 2U, 91U,
                node == 2U));
    EXPECT_TRUE(parsed.ok()) << parsed.error;
    result.push_back(*parsed.view);
  }
  return result;
}

TEST(ChaosVerifierTest, ConvergedRequiresOneReadyLeaderAndEqualAppliedIndexes) {
  auto views = converged_views();
  EXPECT_TRUE(evaluate_convergence(views).ok());
  views[2].last_applied = 90U;
  EXPECT_FALSE(evaluate_convergence(views).ok());
  views = converged_views();
  views[0].ready = true;
  EXPECT_FALSE(evaluate_convergence(views).ok());
}

TEST(ChaosVerifierTest, RejectsDuplicateMissingMalformedAndUnknownRole) {
  EXPECT_FALSE(parse_node_view(1U, true, false,
                               metrics("leader", 1U, 1U, 4U) +
                                   "forgekv_raft_term 2\n")
                   .ok());
  EXPECT_FALSE(parse_node_view(1U, true, false,
                               "forgekv_raft_role{role=\"leader\"} 1\n")
                   .ok());
  EXPECT_FALSE(parse_node_view(1U, true, false,
                               metrics("leader", 1U, 1U, 4U) +
                                   "forgekv_raft_commit_index NaN\n")
                   .ok());
  EXPECT_FALSE(parse_node_view(1U, true, false,
                               metrics("observer", 1U, 1U, 4U))
                   .ok());
}

TEST(ChaosVerifierTest, RejectsLeaderLagAndInconsistentLeaderIdentity) {
  auto views = converged_views();
  views[1].peer_lag[3U] = 1U;
  EXPECT_FALSE(evaluate_convergence(views).ok());
  views = converged_views();
  views[0].leader_id = 3U;
  EXPECT_FALSE(evaluate_convergence(views).ok());
  views = converged_views();
  views[1].peer_lag.erase(3U);
  views[1].peer_lag.emplace(99U, 0U);
  EXPECT_FALSE(evaluate_convergence(views).ok());
}

TEST(ChaosVerifierTest, RequiresAllNodesHealthyAndFullyApplied) {
  auto views = converged_views();
  views[0].healthy = false;
  EXPECT_FALSE(evaluate_convergence(views).ok());
  views = converged_views();
  views[1].commit_index = 92U;
  EXPECT_FALSE(evaluate_convergence(views).ok());
}

}  // namespace
}  // namespace forgekv::chaos
