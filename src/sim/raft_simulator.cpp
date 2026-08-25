#include "sim/raft_simulator.h"

#include <algorithm>
#include <deque>
#include <functional>
#include <iterator>
#include <limits>
#include <map>
#include <random>
#include <set>
#include <sstream>
#include <string_view>
#include <type_traits>
#include <utility>

namespace forgekv::sim {
namespace {

raft::LogicalTime saturating_add(const raft::LogicalTime left,
                                 const raft::LogicalTime right) {
  if (right > std::numeric_limits<raft::LogicalTime>::max() - left) {
    return std::numeric_limits<raft::LogicalTime>::max();
  }
  return left + right;
}

std::pair<raft::NodeId, raft::NodeId> link_key(const raft::NodeId first,
                                               const raft::NodeId second) {
  return std::minmax(first, second);
}

std::string role_name(const raft::Role role) {
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

std::string entry_text(const raft::LogEntry& entry);

std::string message_name(const raft::Message& message) {
  return std::visit(
      [](const auto& value) -> std::string {
        using Value = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<Value, raft::RequestVote>) {
          return "RequestVote(term=" + std::to_string(value.term) +
                 ",candidate=" + std::to_string(value.candidate_id) +
                 ",last_index=" + std::to_string(value.last_log_index) +
                 ",last_term=" + std::to_string(value.last_log_term) + ")";
        } else if constexpr (std::is_same_v<Value,
                                            raft::RequestVoteResponse>) {
          return "RequestVoteResponse(term=" + std::to_string(value.term) +
                 ",grant=" + (value.vote_granted ? "1" : "0") + ")";
        } else if constexpr (std::is_same_v<Value, raft::AppendEntries>) {
          std::ostringstream output;
          output << "AppendEntries(term=" << value.term
                 << ",leader=" << value.leader_id
                 << ",previous_index=" << value.previous_log_index
                 << ",previous_term=" << value.previous_log_term
                 << ",commit=" << value.leader_commit
                 << ",rpc=" << value.rpc_id << ",entries=[";
          for (const auto& entry : value.entries) {
            output << entry_text(entry) << ';';
          }
          output << "])";
          return output.str();
        } else {
          return "AppendEntriesResponse(term=" +
                 std::to_string(value.term) + ",rpc=" +
                 std::to_string(value.rpc_id) + ",success=" +
                 (value.success ? "1" : "0") + ",match=" +
                 std::to_string(value.match_index) + ",reject_hint=" +
                 std::to_string(value.reject_hint) + ")";
        }
      },
      message);
}

std::string entry_text(const raft::LogEntry& entry) {
  std::ostringstream output;
  output << entry.index << ':' << entry.term << ':'
         << (entry.kind == raft::EntryKind::no_op ? "no-op" : "command")
         << ':';
  for (const auto value : entry.command) {
    output << static_cast<unsigned int>(value) << ',';
  }
  return output.str();
}

void validate_simulator_config(const SimulatorConfig& config) {
  if (config.max_pending_messages == 0 || config.max_trace_records == 0) {
    throw std::invalid_argument("simulator bounds must be nonzero");
  }
  if (config.voters.size() < 3 || config.voters.size() > 7 ||
      config.voters.size() % 2 == 0) {
    throw std::invalid_argument("simulator requires 3, 5, or 7 voters");
  }
  const auto maximum_election_burst =
      config.voters.size() * (config.voters.size() - 1);
  if (config.max_pending_messages < maximum_election_burst) {
    throw std::invalid_argument(
        "pending message bound cannot hold one cluster election burst");
  }
}

}  // namespace

InvariantViolation::InvariantViolation(std::string report)
    : std::runtime_error(std::move(report)) {}

struct RaftSimulator::Impl final {
  struct NodeSlot final {
    raft::RaftConfig config;
    std::unique_ptr<raft::RaftNode> node;
    raft::RaftPersistentState persistent;
    std::vector<raft::LogEntry> applied;
    raft::LogIndex highest_commit_seen{};
  };

  explicit Impl(SimulatorConfig value)
      : config(std::move(value)), random(config.seed) {
    validate_simulator_config(config);
    for (const auto node_id : config.voters) {
      raft::RaftConfig node_config{
          .self_id = node_id,
          .voters = config.voters,
          .election_timeout_min = config.election_timeout_min,
          .election_timeout_max = config.election_timeout_max,
          .heartbeat_interval = config.heartbeat_interval,
          .random_seed = config.seed,
      };
      auto node = std::make_unique<raft::RaftNode>(node_config);
      nodes.emplace(node_id, NodeSlot{
                                 .config = std::move(node_config),
                                 .node = std::move(node),
                                 .persistent = {},
                                 .applied = {},
                                 .highest_commit_seen = 0,
                             });
    }
    record("cluster start seed=" + std::to_string(config.seed));
  }

  NodeSlot& slot(const raft::NodeId node_id) {
    const auto found = nodes.find(node_id);
    if (found == nodes.end()) {
      throw std::invalid_argument("unknown simulator node ID");
    }
    return found->second;
  }

  const NodeSlot& slot(const raft::NodeId node_id) const {
    const auto found = nodes.find(node_id);
    if (found == nodes.end()) {
      throw std::invalid_argument("unknown simulator node ID");
    }
    return found->second;
  }

  void record(std::string text) {
    std::ostringstream line;
    line << "op=" << operations << " time=" << logical_time << ' ' << text;
    if (trace_records.size() == config.max_trace_records) {
      trace_records.pop_front();
    }
    trace_records.push_back(line.str());
  }

  [[nodiscard]] bool blocked(const raft::NodeId first,
                             const raft::NodeId second) const {
    return blocked_links.contains(link_key(first, second));
  }

  [[nodiscard]] bool deliverable(const ScheduledMessage& message) const {
    return message.deliver_at <= logical_time &&
           !blocked(message.from, message.to) && slot(message.to).node != nullptr;
  }

  void persist_log(NodeSlot& owner, const raft::PersistLog& persist) {
    if (persist.from_index == 0 ||
        persist.from_index > owner.persistent.log.size() + 1) {
      fail("invalid abstract PersistLog boundary");
    }
    owner.persistent.log.resize(
        static_cast<std::size_t>(persist.from_index - 1));
    owner.persistent.log.insert(owner.persistent.log.end(),
                                persist.entries.begin(), persist.entries.end());
  }

  [[noreturn]] void fail(const std::string_view reason) const {
    throw InvariantViolation(std::string(reason) + "\n" + dump_state());
  }

  void remember_commit(const raft::NodeId source,
                       const raft::LogIndex through) {
    const auto& log = slot(source).persistent.log;
    if (through > log.size()) {
      fail("commit index exceeds durable log");
    }
    for (raft::LogIndex index = 1; index <= through; ++index) {
      const auto& entry = log[static_cast<std::size_t>(index - 1)];
      const auto [found, inserted] = committed_entries.emplace(index, entry);
      if (!inserted && found->second != entry) {
        fail("a committed log index changed value");
      }
    }
  }

  void process_actions(const raft::NodeId source, raft::Actions actions) {
    auto& owner = slot(source);
    const auto send_count = static_cast<std::size_t>(std::ranges::count_if(
        actions, [](const raft::Action& action) {
          return std::holds_alternative<raft::SendMessage>(action);
        }));
    if (send_count > config.max_pending_messages - pending.size()) {
      fail("pending message bound exceeded");
    }
    for (auto& action : actions) {
      std::visit(
          [this, source, &owner](auto& value) {
            using Value = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<Value, raft::SendMessage>) {
              const auto id = next_message_id++;
              pending.push_back(ScheduledMessage{
                  .id = id,
                  .from = source,
                  .to = value.to,
                  .message = std::move(value.message),
                  .created_at = logical_time,
                  .deliver_at = logical_time,
              });
              record("enqueue id=" + std::to_string(id) + " " +
                     std::to_string(source) + "->" +
                     std::to_string(value.to) + " " +
                     message_name(pending.back().message));
            } else if constexpr (std::is_same_v<Value,
                                                raft::PersistHardState>) {
              owner.persistent.current_term = value.term;
              owner.persistent.voted_for = value.voted_for;
              record("persist-hard node=" + std::to_string(source) +
                     " term=" + std::to_string(value.term));
            } else if constexpr (std::is_same_v<Value, raft::PersistLog>) {
              persist_log(owner, value);
              record("persist-log node=" + std::to_string(source) +
                     " from=" + std::to_string(value.from_index) +
                     " count=" + std::to_string(value.entries.size()));
            } else if constexpr (std::is_same_v<Value, raft::RoleChanged>) {
              if (value.to == raft::Role::leader) {
                const auto [found, inserted] =
                    elected_leaders.emplace(value.term, source);
                if (!inserted && found->second != source) {
                  fail("two leaders were elected in one term");
                }
              }
              record("role node=" + std::to_string(source) + " " +
                     role_name(value.from) + "->" + role_name(value.to) +
                     " term=" + std::to_string(value.term));
            } else if constexpr (std::is_same_v<Value,
                                                raft::CommitAdvanced>) {
              owner.highest_commit_seen =
                  std::max(owner.highest_commit_seen, value.to_index);
              remember_commit(source, value.to_index);
              record("commit node=" + std::to_string(source) + " through=" +
                     std::to_string(value.to_index));
            } else if constexpr (std::is_same_v<Value, raft::ApplyEntry>) {
              const auto index = value.entry.index;
              if (index == 0 || index > owner.persistent.log.size() ||
                  owner.persistent.log[static_cast<std::size_t>(index - 1)] !=
                      value.entry) {
                fail("applied entry does not match the durable local log");
              }
              const auto committed = committed_entries.find(index);
              if (committed == committed_entries.end() ||
                  committed->second != value.entry) {
                fail("applied entry is not the globally committed value");
              }
              if (index <= owner.applied.size()) {
                if (owner.applied[static_cast<std::size_t>(index - 1)] !=
                    value.entry) {
                  fail("applied entry changed on replay");
                }
              } else if (index == owner.applied.size() + 1) {
                owner.applied.push_back(value.entry);
              } else {
                fail("application skipped a log index");
              }
              record("apply node=" + std::to_string(source) + " index=" +
                     std::to_string(index));
            } else {
              record("proposal-rejected node=" + std::to_string(source));
            }
          },
          action);
    }
  }

  void verify_invariants() const {
    std::map<raft::Term, raft::NodeId> current_leaders;
    for (const auto& [node_id, owner] : nodes) {
      for (std::size_t offset = 0; offset < owner.persistent.log.size();
           ++offset) {
        const auto& entry = owner.persistent.log[offset];
        if (entry.index != offset + 1 || entry.term == 0 ||
            entry.term > owner.persistent.current_term ||
            (offset > 0 &&
             entry.term < owner.persistent.log[offset - 1].term)) {
          fail("durable log structure is invalid");
        }
      }
      if (owner.highest_commit_seen > owner.persistent.log.size()) {
        fail("a node lost an entry it had committed");
      }
      for (raft::LogIndex index = 1; index <= owner.highest_commit_seen;
           ++index) {
        const auto known = committed_entries.find(index);
        if (known == committed_entries.end() ||
            known->second !=
                owner.persistent.log[static_cast<std::size_t>(index - 1)]) {
          fail("a node changed an entry it had committed");
        }
      }
      if (owner.node == nullptr) {
        continue;
      }
      const auto state = owner.node->snapshot();
      if (state.commit_index > state.log.size() ||
          state.last_applied > state.commit_index) {
        fail("node applied beyond commit index or committed beyond its log");
      }
      if (state.log != owner.persistent.log ||
          state.current_term != owner.persistent.current_term ||
          state.voted_for != owner.persistent.voted_for) {
        fail("volatile Raft view disagrees with abstract durable state");
      }
      if (state.role == raft::Role::leader) {
        const auto [found, inserted] =
            current_leaders.emplace(state.current_term, node_id);
        if (!inserted && found->second != node_id) {
          fail("two active leaders exist in one term");
        }
      }
      for (raft::LogIndex index = 1; index <= state.commit_index; ++index) {
        const auto known = committed_entries.find(index);
        if (known != committed_entries.end() &&
            known->second != state.log[static_cast<std::size_t>(index - 1)]) {
          fail("committed logs disagree");
        }
      }
    }

    for (auto left = nodes.begin(); left != nodes.end(); ++left) {
      for (auto right = std::next(left); right != nodes.end(); ++right) {
        const auto common_log =
            std::min(left->second.persistent.log.size(),
                     right->second.persistent.log.size());
        for (std::size_t offset = 0; offset < common_log; ++offset) {
          const auto& first = left->second.persistent.log[offset];
          const auto& second = right->second.persistent.log[offset];
          if (first.term == second.term) {
            for (std::size_t prefix = 0; prefix <= offset; ++prefix) {
              if (left->second.persistent.log[prefix] !=
                  right->second.persistent.log[prefix]) {
                fail("log matching property was violated");
              }
            }
          }
        }
        const auto common_applied =
            std::min(left->second.applied.size(), right->second.applied.size());
        for (std::size_t offset = 0; offset < common_applied; ++offset) {
          if (left->second.applied[offset] != right->second.applied[offset]) {
            fail("applied state machines disagree at a common index");
          }
        }
      }
    }
  }

  [[nodiscard]] std::string dump_state() const {
    std::ostringstream output;
    output << "seed=" << config.seed << " operations=" << operations
           << " time=" << logical_time << '\n';
    for (const auto& record : trace_records) {
      output << record << '\n';
    }
    for (const auto& [node_id, owner] : nodes) {
      output << "node=" << node_id << " active=" << (owner.node ? 1 : 0)
             << " durable_term=" << owner.persistent.current_term
             << " voted_for=";
      if (owner.persistent.voted_for.has_value()) {
        output << *owner.persistent.voted_for;
      } else {
        output << "none";
      }
      output << " highest_commit=" << owner.highest_commit_seen << '\n';
      if (owner.node) {
        const auto state = owner.node->snapshot();
        output << "  role=" << role_name(state.role)
               << " term=" << state.current_term
               << " commit=" << state.commit_index
               << " applied=" << state.last_applied
               << " deadline=" << state.election_deadline << '\n';
      }
      output << "  log";
      for (const auto& entry : owner.persistent.log) {
        output << ' ' << entry_text(entry);
      }
      output << '\n';
      output << "  applied-history";
      for (const auto& entry : owner.applied) {
        output << ' ' << entry_text(entry);
      }
      output << '\n';
    }
    output << "blocked-links";
    for (const auto& [first, second] : blocked_links) {
      output << ' ' << first << "<->" << second;
    }
    output << '\n';
    output << "elected-leaders";
    for (const auto& [term, node_id] : elected_leaders) {
      output << " term=" << term << ":node=" << node_id;
    }
    output << '\n';
    output << "committed-history";
    for (const auto& [index, entry] : committed_entries) {
      static_cast<void>(index);
      output << ' ' << entry_text(entry);
    }
    output << '\n';
    for (const auto& message : pending) {
      output << "pending id=" << message.id << ' ' << message.from << "->"
             << message.to << " created=" << message.created_at
             << " deliver=" << message.deliver_at << ' '
             << message_name(message.message) << '\n';
    }
    return output.str();
  }

  SimulatorConfig config;
  raft::LogicalTime logical_time{};
  std::uint64_t operations{};
  std::uint64_t next_message_id{1};
  std::map<raft::NodeId, NodeSlot> nodes;
  std::vector<ScheduledMessage> pending;
  std::set<std::pair<raft::NodeId, raft::NodeId>> blocked_links;
  std::deque<std::string> trace_records;
  std::map<raft::Term, raft::NodeId> elected_leaders;
  std::map<raft::LogIndex, raft::LogEntry> committed_entries;
  std::mt19937_64 random;
};

RaftSimulator::RaftSimulator(SimulatorConfig config)
    : impl_(std::make_unique<Impl>(std::move(config))) {}

RaftSimulator::~RaftSimulator() = default;
RaftSimulator::RaftSimulator(RaftSimulator&&) noexcept = default;
RaftSimulator& RaftSimulator::operator=(RaftSimulator&&) noexcept = default;

raft::LogicalTime RaftSimulator::now() const {
  return impl_->logical_time;
}

std::uint64_t RaftSimulator::seed() const {
  return impl_->config.seed;
}

std::uint64_t RaftSimulator::operation_count() const {
  return impl_->operations;
}

SimulatorNodeView RaftSimulator::node(const raft::NodeId node_id) const {
  const auto& owner = impl_->slot(node_id);
  return SimulatorNodeView{
      .active = owner.node != nullptr,
      .state = owner.node != nullptr
                   ? std::optional<raft::RaftSnapshot>{owner.node->snapshot()}
                   : std::nullopt,
      .persistent = owner.persistent,
      .applied = owner.applied,
      .highest_commit_seen = owner.highest_commit_seen,
  };
}

std::vector<ScheduledMessage> RaftSimulator::pending_messages() const {
  return impl_->pending;
}

void RaftSimulator::advance_time_to(const raft::LogicalTime time) {
  if (time < impl_->logical_time) {
    throw std::invalid_argument("simulator logical time cannot move backward");
  }
  impl_->logical_time = time;
  ++impl_->operations;
  impl_->record("advance-time");
  for (auto& [node_id, owner] : impl_->nodes) {
    if (owner.node != nullptr) {
      impl_->process_actions(node_id, owner.node->advance_time(time));
    }
  }
  check_invariants();
}

bool RaftSimulator::deliver(const std::uint64_t message_id) {
  const auto found = std::ranges::find_if(
      impl_->pending, [message_id](const ScheduledMessage& message) {
        return message.id == message_id;
      });
  if (found == impl_->pending.end()) {
    throw std::invalid_argument("unknown pending message ID");
  }
  if (!impl_->deliverable(*found)) {
    return false;
  }
  auto message = std::move(*found);
  impl_->pending.erase(found);
  ++impl_->operations;
  impl_->record("deliver id=" + std::to_string(message.id) + " " +
                message_name(message.message));
  auto& target = impl_->slot(message.to);
  impl_->process_actions(
      message.to, target.node->step(message.from, message.message));
  check_invariants();
  return true;
}

std::size_t RaftSimulator::deliver_all(const std::size_t limit) {
  std::size_t delivered = 0;
  while (delivered < limit) {
    const auto found = std::ranges::find_if(
        impl_->pending, [this](const ScheduledMessage& message) {
          return impl_->deliverable(message);
        });
    if (found == impl_->pending.end()) {
      break;
    }
    const auto id = found->id;
    static_cast<void>(deliver(id));
    ++delivered;
  }
  return delivered;
}

void RaftSimulator::delay(const std::uint64_t message_id,
                          const raft::LogicalTime delay_amount) {
  const auto found = std::ranges::find_if(
      impl_->pending, [message_id](const ScheduledMessage& message) {
        return message.id == message_id;
      });
  if (found == impl_->pending.end()) {
    throw std::invalid_argument("unknown pending message ID");
  }
  found->deliver_at = saturating_add(
      std::max(found->deliver_at, impl_->logical_time), delay_amount);
  ++impl_->operations;
  impl_->record("delay id=" + std::to_string(message_id) + " until=" +
                std::to_string(found->deliver_at));
  check_invariants();
}

void RaftSimulator::drop(const std::uint64_t message_id) {
  const auto found = std::ranges::find_if(
      impl_->pending, [message_id](const ScheduledMessage& message) {
        return message.id == message_id;
      });
  if (found == impl_->pending.end()) {
    throw std::invalid_argument("unknown pending message ID");
  }
  impl_->pending.erase(found);
  ++impl_->operations;
  impl_->record("drop id=" + std::to_string(message_id));
  check_invariants();
}

void RaftSimulator::block_link(const raft::NodeId first,
                               const raft::NodeId second) {
  static_cast<void>(impl_->slot(first));
  static_cast<void>(impl_->slot(second));
  if (first == second) {
    throw std::invalid_argument("cannot block a node's self link");
  }
  impl_->blocked_links.insert(link_key(first, second));
  ++impl_->operations;
  impl_->record("block " + std::to_string(first) + "<->" +
                std::to_string(second));
  check_invariants();
}

void RaftSimulator::heal_link(const raft::NodeId first,
                              const raft::NodeId second) {
  static_cast<void>(impl_->slot(first));
  static_cast<void>(impl_->slot(second));
  impl_->blocked_links.erase(link_key(first, second));
  ++impl_->operations;
  impl_->record("heal " + std::to_string(first) + "<->" +
                std::to_string(second));
  check_invariants();
}

void RaftSimulator::isolate(const raft::NodeId node_id) {
  static_cast<void>(impl_->slot(node_id));
  for (const auto peer : impl_->config.voters) {
    if (peer != node_id) {
      impl_->blocked_links.insert(link_key(node_id, peer));
    }
  }
  ++impl_->operations;
  impl_->record("isolate node=" + std::to_string(node_id));
  check_invariants();
}

void RaftSimulator::heal_all() {
  impl_->blocked_links.clear();
  ++impl_->operations;
  impl_->record("heal-all");
  check_invariants();
}

void RaftSimulator::crash(const raft::NodeId node_id) {
  auto& owner = impl_->slot(node_id);
  owner.node.reset();
  ++impl_->operations;
  impl_->record("crash node=" + std::to_string(node_id));
  check_invariants();
}

void RaftSimulator::restart(const raft::NodeId node_id) {
  auto& owner = impl_->slot(node_id);
  if (owner.node != nullptr) {
    throw std::invalid_argument("cannot restart an active simulator node");
  }
  owner.node = std::make_unique<raft::RaftNode>(
      owner.config, owner.persistent, impl_->logical_time);
  ++impl_->operations;
  impl_->record("restart node=" + std::to_string(node_id));
  check_invariants();
}

void RaftSimulator::propose(const raft::NodeId node_id,
                            std::vector<std::byte> command) {
  auto& owner = impl_->slot(node_id);
  if (owner.node == nullptr) {
    throw std::invalid_argument("cannot propose to a crashed simulator node");
  }
  ++impl_->operations;
  impl_->record("propose node=" + std::to_string(node_id) + " bytes=" +
                std::to_string(command.size()));
  impl_->process_actions(node_id, owner.node->propose(std::move(command)));
  check_invariants();
}

void RaftSimulator::run_random(const std::size_t steps) {
  const auto choose = [this](const std::size_t count) {
    return static_cast<std::size_t>(impl_->random() % count);
  };
  const auto advance = [this]() {
    advance_time_to(saturating_add(
        now(), 1 + static_cast<raft::LogicalTime>(impl_->random() % 50)));
  };

  for (std::size_t step = 0; step < steps; ++step) {
    std::vector<raft::NodeId> active;
    std::vector<raft::NodeId> crashed;
    std::vector<std::uint64_t> deliverable;
    for (const auto& [node_id, owner] : impl_->nodes) {
      (owner.node != nullptr ? active : crashed).push_back(node_id);
    }
    for (const auto& message : impl_->pending) {
      if (impl_->deliverable(message)) {
        deliverable.push_back(message.id);
      }
    }

    const auto maximum_election_burst =
        impl_->config.voters.size() * (impl_->config.voters.size() - 1);
    if (impl_->pending.size() >
        impl_->config.max_pending_messages - maximum_election_burst) {
      if (!deliverable.empty() && (impl_->random() & 1U) != 0U) {
        static_cast<void>(deliver(deliverable[choose(deliverable.size())]));
      } else {
        drop(impl_->pending[choose(impl_->pending.size())].id);
      }
      continue;
    }

    switch (impl_->random() % 10) {
      case 0:
        advance();
        break;
      case 1:
        if (!deliverable.empty()) {
          static_cast<void>(deliver(deliverable[choose(deliverable.size())]));
        } else {
          advance();
        }
        break;
      case 2:
        if (!impl_->pending.empty()) {
          drop(impl_->pending[choose(impl_->pending.size())].id);
        } else {
          advance();
        }
        break;
      case 3:
        if (!impl_->pending.empty()) {
          delay(impl_->pending[choose(impl_->pending.size())].id,
                1 + static_cast<raft::LogicalTime>(impl_->random() % 100));
        } else {
          advance();
        }
        break;
      case 4: {
        const auto first = impl_->config.voters[choose(impl_->config.voters.size())];
        auto second = first;
        while (second == first) {
          second = impl_->config.voters[choose(impl_->config.voters.size())];
        }
        block_link(first, second);
        break;
      }
      case 5:
        heal_all();
        break;
      case 6:
        if (active.size() > 1) {
          crash(active[choose(active.size())]);
        } else {
          advance();
        }
        break;
      case 7:
        if (!crashed.empty()) {
          restart(crashed[choose(crashed.size())]);
        } else {
          advance();
        }
        break;
      case 8:
        if (!active.empty()) {
          auto target = active[choose(active.size())];
          for (const auto node_id : active) {
            if (impl_->slot(node_id).node->snapshot().role ==
                raft::Role::leader) {
              target = node_id;
              break;
            }
          }
          const auto value = static_cast<unsigned char>(impl_->random());
          propose(target, {static_cast<std::byte>(value)});
        } else {
          restart(crashed[choose(crashed.size())]);
        }
        break;
      default:
        if (!active.empty()) {
          isolate(active[choose(active.size())]);
        } else {
          restart(crashed[choose(crashed.size())]);
        }
        break;
    }
  }
}

void RaftSimulator::check_invariants() const {
  impl_->verify_invariants();
}

std::string RaftSimulator::trace() const {
  std::ostringstream output;
  for (const auto& record : impl_->trace_records) {
    output << record << '\n';
  }
  return output.str();
}

std::string RaftSimulator::dump() const {
  return impl_->dump_state();
}

}  // namespace forgekv::sim
