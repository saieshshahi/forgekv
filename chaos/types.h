#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

namespace forgekv::chaos {

enum class NodeState : std::uint8_t { dead, running, paused };

struct NodeView final {
  std::uint64_t id{};
  NodeState state{NodeState::dead};
  bool operator==(const NodeView&) const = default;
};

struct ClusterView final {
  std::vector<NodeView> nodes;
  std::optional<std::uint64_t> leader;
};

enum class ActionKind : std::uint8_t {
  no_op,
  kill_leader,
  kill_follower,
  restart_node,
  partition_node,
  partition_leader_majority,
  set_latency,
  set_jitter,
  set_loss,
  heal_network,
  pause_node,
  resume_node,
  rapid_leader_churn,
};

struct ChaosAction final {
  ActionKind kind{ActionKind::no_op};
  std::uint64_t planned_offset_us{};
  std::uint64_t node{};
  std::uint64_t peer{};
  std::uint32_t value{};
  bool operator==(const ChaosAction&) const = default;
};

struct SchedulerLimits final {
  std::uint32_t maximum_parameter{1'000U};
};

}  // namespace forgekv::chaos
