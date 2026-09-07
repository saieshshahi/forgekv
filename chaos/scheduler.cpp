#include "chaos/scheduler.h"

#include <algorithm>
#include <stdexcept>
#include <vector>

namespace forgekv::chaos {
namespace {

const NodeView* find_node(const ClusterView& view, const std::uint64_t id) {
  const auto found = std::ranges::find(view.nodes, id, &NodeView::id);
  return found == view.nodes.end() ? nullptr : &*found;
}

}  // namespace

ChaosScheduler::ChaosScheduler(const std::uint64_t seed,
                               const SchedulerLimits limits)
    : state_(seed == 0U ? 0x9E3779B97F4A7C15ULL : seed), limits_(limits) {
  if (limits_.maximum_parameter == 0U) {
    throw std::invalid_argument("scheduler parameter bound must be positive");
  }
}

std::uint64_t ChaosScheduler::random() {
  state_ ^= state_ >> 12U;
  state_ ^= state_ << 25U;
  state_ ^= state_ >> 27U;
  return state_ * 0x2545F4914F6CDD1DULL;
}

std::uint64_t ChaosScheduler::choose(const std::uint64_t count) {
  return count == 0U ? 0U : random() % count;
}

ChaosAction ChaosScheduler::next(const std::uint64_t planned_offset_us,
                                 const ClusterView& view) {
  if (view.nodes.empty()) {
    return ChaosAction{.planned_offset_us = planned_offset_us};
  }
  constexpr auto kActionCount =
      static_cast<std::uint64_t>(ActionKind::rapid_leader_churn) + 1U;
  ChaosAction action{
      .kind = static_cast<ActionKind>(choose(kActionCount - 1U) + 1U),
      .planned_offset_us = planned_offset_us,
      .node = view.nodes[static_cast<std::size_t>(choose(view.nodes.size()))].id,
      .peer = view.nodes[static_cast<std::size_t>(choose(view.nodes.size()))].id,
      .value = static_cast<std::uint32_t>(
          choose(static_cast<std::uint64_t>(limits_.maximum_parameter) + 1U)),
  };
  if (action.peer == action.node && view.nodes.size() > 1U) {
    const auto found = std::ranges::find(view.nodes, action.node,
                                         &NodeView::id);
    const auto position = static_cast<std::size_t>(found - view.nodes.begin());
    action.peer = view.nodes[(position + 1U) % view.nodes.size()].id;
  }
  return normalize(action, view);
}

ChaosAction ChaosScheduler::normalize(ChaosAction action,
                                      const ClusterView& view) const {
  const auto no_op = [&] {
    action.kind = ActionKind::no_op;
    action.node = 0U;
    action.peer = 0U;
    action.value = 0U;
    return action;
  };
  const auto* target = find_node(view, action.node);
  switch (action.kind) {
    case ActionKind::kill_leader:
    case ActionKind::partition_leader_majority:
      if (!view.leader.has_value()) {
        return no_op();
      }
      action.node = *view.leader;
      target = find_node(view, action.node);
      return target != nullptr && target->state == NodeState::running ? action
                                                                      : no_op();
    case ActionKind::rapid_leader_churn: {
      if (!view.leader.has_value()) {
        return no_op();
      }
      action.node = *view.leader;
      target = find_node(view, action.node);
      const auto running = static_cast<std::size_t>(std::ranges::count(
          view.nodes, NodeState::running, &NodeView::state));
      const auto live_replacements = running == 0U ? 0U : running - 1U;
      const auto quorum = view.nodes.size() / 2U + 1U;
      return target != nullptr && target->state == NodeState::running &&
                     live_replacements >= quorum
                 ? action
                 : no_op();
    }
    case ActionKind::kill_follower: {
      const auto found = std::ranges::find_if(view.nodes, [&](const NodeView& node) {
        return node.state == NodeState::running &&
               (!view.leader.has_value() || node.id != *view.leader);
      });
      if (found == view.nodes.end()) {
        return no_op();
      }
      action.node = found->id;
      return action;
    }
    case ActionKind::restart_node:
      return target != nullptr && target->state == NodeState::dead ? action
                                                                   : no_op();
    case ActionKind::pause_node:
      return target != nullptr && target->state == NodeState::running ? action
                                                                      : no_op();
    case ActionKind::resume_node:
      return target != nullptr && target->state == NodeState::paused ? action
                                                                     : no_op();
    case ActionKind::partition_node:
      return target != nullptr && target->state != NodeState::dead ? action
                                                                   : no_op();
    case ActionKind::set_latency:
    case ActionKind::set_jitter:
      return target != nullptr && find_node(view, action.peer) != nullptr &&
                     action.node != action.peer
                 ? action
                 : no_op();
    case ActionKind::set_loss:
      if (target == nullptr || find_node(view, action.peer) == nullptr ||
          action.node == action.peer) {
        return no_op();
      }
      action.value = std::min(action.value, 100U);
      return action;
    case ActionKind::heal_network:
    case ActionKind::no_op:
      return action;
  }
  return no_op();
}

}  // namespace forgekv::chaos
