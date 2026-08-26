#include "raft/persisted_raft_node.h"

#include <algorithm>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace forgekv::raft {

std::uint64_t fixed_membership_fingerprint(std::vector<NodeId> voters) {
  std::ranges::sort(voters);
  std::uint64_t hash = 0xCBF29CE484222325ULL;
  for (const auto voter : voters) {
    for (unsigned int shift = 0; shift < 64U; shift += 8U) {
      hash ^= (voter >> shift) & 0xFFU;
      hash *= 0x100000001B3ULL;
    }
  }
  hash ^= voters.size();
  hash *= 0x100000001B3ULL;
  return hash == 0 ? 1 : hash;
}

struct PersistedRaftNode::Impl final {
  Impl(RaftConfig config, RaftStorage durable_storage,
       const LogicalTime initial_time, RaftOutputSink output_sink,
       std::shared_ptr<RaftCrashHook> hook)
      : storage(std::move(durable_storage)),
        node(std::move(config), storage.state(), initial_time),
        output(std::move(output_sink)),
        crash_hook(std::move(hook)) {}

  void ensure_healthy() const {
    if (is_failed) {
      throw std::logic_error("persisted Raft driver is faulted");
    }
  }

  void at(const RaftCrashPoint point) const {
    if (crash_hook && *crash_hook) {
      (*crash_hook)(point);
    }
  }

  template <typename PersistentAction>
  void persist(const PersistentAction& action) {
    at(RaftCrashPoint::before_persist);
    storage.prepare(action);
    at(RaftCrashPoint::after_write);
    storage.sync();
    at(RaftCrashPoint::after_sync);
  }

  void drive(Actions actions) {
    ensure_healthy();
    Actions publish;
    publish.reserve(actions.size());
    try {
      for (auto& action : actions) {
        if (const auto* hard = std::get_if<PersistHardState>(&action)) {
          persist(*hard);
        } else if (const auto* log = std::get_if<PersistLog>(&action)) {
          persist(*log);
        } else {
          publish.push_back(std::move(action));
        }
      }

      for (const auto& action : publish) {
        const bool is_message = std::holds_alternative<SendMessage>(action);
        if (is_message) {
          at(RaftCrashPoint::before_response);
        }
        output(action);
        if (is_message) {
          at(RaftCrashPoint::after_response);
        }
      }
    } catch (...) {
      is_failed = true;
      throw;
    }
  }

  RaftStorage storage;
  RaftNode node;
  RaftOutputSink output;
  std::shared_ptr<RaftCrashHook> crash_hook;
  bool is_failed{};
};

PersistedRaftNode PersistedRaftNode::open(PersistedRaftOptions options) {
  if (!options.output) {
    throw std::invalid_argument("persisted Raft driver requires an output sink");
  }
  validate_raft_config(options.config);
  auto shared_crash_hook =
      std::make_shared<RaftCrashHook>(std::move(options.crash_hook));
  const auto voter_fingerprint =
      fixed_membership_fingerprint(options.config.voters);
  auto storage =
      RaftStorage::open(options.data_directory, options.config.cluster_id,
                        options.config.self_id,
                        [hook = shared_crash_hook](
                            const RaftStorageSyncPoint point) {
                          if (!*hook) {
                            return;
                          }
                          (*hook)(point == RaftStorageSyncPoint::after_file_sync
                                      ? RaftCrashPoint::after_file_sync
                                      : RaftCrashPoint::after_rename);
                        },
                        voter_fingerprint);
  return PersistedRaftNode(std::make_unique<Impl>(
      std::move(options.config), std::move(storage), options.initial_time,
      std::move(options.output), std::move(shared_crash_hook)));
}

PersistedRaftNode::PersistedRaftNode(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}
PersistedRaftNode::~PersistedRaftNode() = default;
PersistedRaftNode::PersistedRaftNode(PersistedRaftNode&&) noexcept = default;
PersistedRaftNode& PersistedRaftNode::operator=(PersistedRaftNode&&) noexcept =
    default;

void PersistedRaftNode::advance_time(const LogicalTime now) {
  impl_->ensure_healthy();
  impl_->drive(impl_->node.advance_time(now));
}

void PersistedRaftNode::step(const NodeId from, const Message& message) {
  impl_->ensure_healthy();
  impl_->drive(impl_->node.step(from, message));
}

void PersistedRaftNode::propose(std::vector<std::byte> command) {
  impl_->ensure_healthy();
  impl_->drive(impl_->node.propose(std::move(command)));
}

void PersistedRaftNode::read_barrier() {
  impl_->ensure_healthy();
  impl_->drive(impl_->node.read_barrier());
}

RaftSnapshot PersistedRaftNode::snapshot() const {
  return impl_->node.snapshot();
}

RaftPersistentState PersistedRaftNode::durable_state() const {
  return impl_->storage.state();
}

bool PersistedRaftNode::failed() const noexcept {
  return !impl_ || impl_->is_failed;
}

}  // namespace forgekv::raft
