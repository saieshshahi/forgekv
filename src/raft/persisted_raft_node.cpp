#include "raft/persisted_raft_node.h"

#include <algorithm>
#include <chrono>
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
       std::shared_ptr<RaftCrashHook> hook, RaftSyncObserver observer)
      : storage(std::move(durable_storage)),
        node(std::move(config), storage.state(), initial_time),
        output(std::move(output_sink)),
        crash_hook(std::move(hook)), sync_observer(std::move(observer)) {}

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
    const auto sync_started = std::chrono::steady_clock::now();
    storage.sync();
    observe_sync(sync_started);
    at(RaftCrashPoint::after_sync);
  }

  void observe_sync(
      const std::chrono::steady_clock::time_point started) const noexcept {
    if (!sync_observer) {
      return;
    }
    try {
      sync_observer(std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now() - started));
    } catch (...) {
      // Telemetry is passive and cannot change a durability decision.
    }
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
        } else if (const auto* snapshot =
                       std::get_if<PersistSnapshot>(&action)) {
          at(RaftCrashPoint::before_persist);
          const auto sync_started = std::chrono::steady_clock::now();
          storage.install_snapshot(snapshot->snapshot,
                                   snapshot_already_durable);
          observe_sync(sync_started);
          snapshot_already_durable = false;
          at(RaftCrashPoint::after_sync);
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
  RaftSyncObserver sync_observer;
  bool is_failed{};
  bool snapshot_already_durable{};
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
                          switch (point) {
                            case RaftStorageSyncPoint::after_write:
                              (*hook)(RaftCrashPoint::after_write);
                              break;
                            case RaftStorageSyncPoint::after_file_sync:
                              (*hook)(RaftCrashPoint::after_file_sync);
                              break;
                            case RaftStorageSyncPoint::after_rename:
                              (*hook)(RaftCrashPoint::after_rename);
                              break;
                            case RaftStorageSyncPoint::after_directory_sync:
                              (*hook)(RaftCrashPoint::after_sync);
                              break;
                          }
                        },
                        voter_fingerprint);
  return PersistedRaftNode(std::make_unique<Impl>(
      std::move(options.config), std::move(storage), options.initial_time,
      std::move(options.output), std::move(shared_crash_hook),
      std::move(options.sync_observer)));
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

void PersistedRaftNode::compact(StateMachineSnapshot snapshot,
                                const bool snapshot_already_durable) {
  impl_->ensure_healthy();
  impl_->snapshot_already_durable = snapshot_already_durable;
  try {
    impl_->drive(impl_->node.compact(std::move(snapshot)));
  } catch (...) {
    impl_->snapshot_already_durable = false;
    throw;
  }
}

RaftSnapshot PersistedRaftNode::snapshot() const {
  return impl_->node.snapshot();
}

RaftStatus PersistedRaftNode::status() const noexcept {
  return impl_->node.status();
}

std::optional<PeerProgress> PersistedRaftNode::progress(
    const NodeId peer) const {
  return impl_->node.progress(peer);
}

std::optional<LogIndex> PersistedRaftNode::match_index(
    const NodeId peer) const noexcept {
  return impl_->node.match_index(peer);
}

RaftPersistentState PersistedRaftNode::durable_state() const {
  return impl_->storage.state();
}

bool PersistedRaftNode::failed() const noexcept {
  return !impl_ || impl_->is_failed;
}

}  // namespace forgekv::raft
