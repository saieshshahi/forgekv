#include "chaos/scheduler.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

namespace forgekv::chaos {
namespace {

ClusterView cluster_view(const std::size_t nodes,
                         const std::optional<std::uint64_t> leader) {
  ClusterView view{.nodes = {}, .leader = leader};
  for (std::uint64_t node = 1; node <= nodes; ++node) {
    view.nodes.push_back(NodeView{.id = node, .state = NodeState::running});
  }
  return view;
}

TEST(ChaosSchedulerTest, SameSeedProducesIdenticalBoundedTimeline) {
  ChaosScheduler left(12345U, SchedulerLimits{});
  ChaosScheduler right(12345U, SchedulerLimits{});
  const auto view = cluster_view(5U, 2U);

  for (std::uint64_t step = 0; step < 500U; ++step) {
    const auto first = left.next(step * 100'000U, view);
    const auto second = right.next(step * 100'000U, view);
    EXPECT_EQ(first, second);
    EXPECT_EQ(first.planned_offset_us, step * 100'000U);
    EXPECT_LE(first.value, SchedulerLimits{}.maximum_parameter);
  }
}

TEST(ChaosSchedulerTest, DifferentSeedsProduceDifferentTimeline) {
  ChaosScheduler left(1U, SchedulerLimits{});
  ChaosScheduler right(2U, SchedulerLimits{});
  const auto view = cluster_view(5U, 2U);
  std::vector<ChaosAction> first;
  std::vector<ChaosAction> second;
  for (std::uint64_t step = 0; step < 32U; ++step) {
    first.push_back(left.next(step, view));
    second.push_back(right.next(step, view));
  }
  EXPECT_NE(first, second);
}

TEST(ChaosSchedulerTest, NormalizesInapplicableLifecycleActionsToNoOp) {
  ChaosScheduler scheduler(7U, SchedulerLimits{});
  const auto all_running = cluster_view(3U, 1U);
  EXPECT_EQ(scheduler.normalize(
                ChaosAction{.kind = ActionKind::restart_node, .node = 2U},
                all_running)
                .kind,
            ActionKind::no_op);
  EXPECT_EQ(scheduler.normalize(
                ChaosAction{.kind = ActionKind::resume_node, .node = 2U},
                all_running)
                .kind,
            ActionKind::no_op);

  auto one_dead = all_running;
  one_dead.nodes[1].state = NodeState::dead;
  EXPECT_EQ(scheduler.normalize(
                ChaosAction{.kind = ActionKind::pause_node, .node = 2U},
                one_dead)
                .kind,
            ActionKind::no_op);
  EXPECT_EQ(scheduler.normalize(
                ChaosAction{.kind = ActionKind::restart_node, .node = 2U},
                one_dead)
                .kind,
            ActionKind::restart_node);
}

TEST(ChaosSchedulerTest, RejectsInvalidLimitsAndZeroSeedRemainsStable) {
  auto invalid = SchedulerLimits{};
  invalid.maximum_parameter = 0U;
  EXPECT_THROW(ChaosScheduler(1U, invalid), std::invalid_argument);

  ChaosScheduler left(0U, SchedulerLimits{});
  ChaosScheduler right(0U, SchedulerLimits{});
  EXPECT_EQ(left.next(10U, cluster_view(3U, 1U)),
            right.next(10U, cluster_view(3U, 1U)));
}

TEST(ChaosSchedulerTest, MissingLeaderNodeNormalizesWithoutDereference) {
  ChaosScheduler scheduler(9U, SchedulerLimits{});
  const auto view = cluster_view(3U, 99U);
  EXPECT_EQ(scheduler.normalize(
                ChaosAction{.kind = ActionKind::kill_leader}, view)
                .kind,
            ActionKind::no_op);
}

TEST(ChaosSchedulerTest, RecordsTheEffectiveBoundedLossPercentage) {
  ChaosScheduler scheduler(11U, SchedulerLimits{});
  const auto normalized = scheduler.normalize(
      ChaosAction{.kind = ActionKind::set_loss,
                  .node = 1U,
                  .peer = 2U,
                  .value = 474U},
      cluster_view(3U, 1U));
  EXPECT_EQ(normalized.kind, ActionKind::set_loss);
  EXPECT_EQ(normalized.value, 100U);
}

TEST(ChaosSchedulerTest, RapidChurnRequiresAReplacementElectionQuorum) {
  ChaosScheduler scheduler(13U, SchedulerLimits{});
  auto view = cluster_view(3U, 1U);
  view.nodes[2].state = NodeState::dead;
  EXPECT_EQ(scheduler.normalize(
                ChaosAction{.kind = ActionKind::rapid_leader_churn}, view)
                .kind,
            ActionKind::no_op);

  view = cluster_view(5U, 1U);
  view.nodes[4].state = NodeState::paused;
  EXPECT_EQ(scheduler.normalize(
                ChaosAction{.kind = ActionKind::rapid_leader_churn}, view)
                .kind,
            ActionKind::rapid_leader_churn);
}

}  // namespace
}  // namespace forgekv::chaos
