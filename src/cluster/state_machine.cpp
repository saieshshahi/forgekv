#include "cluster/state_machine.h"

#include "protocol/frame.h"

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace forgekv::cluster {
namespace {

void validate_response(const ReplicatedCommand& command,
                       const std::vector<std::byte>& response) {
  if (command.operation == KvOperation::put) {
    if (!response.empty()) {
      throw std::invalid_argument("PUT deduplication result is not empty");
    }
    return;
  }
  if (response.size() != 1U ||
      (response[0] != std::byte{0} && response[0] != std::byte{1})) {
    throw std::invalid_argument("DELETE deduplication result is invalid");
  }
}

std::size_t validate_snapshot_state(const SnapshotState& state,
                                    const std::size_t max_clients,
                                    const std::size_t max_dedup_bytes) {
  if (state.clients.size() > max_clients) {
    throw std::invalid_argument("snapshot exceeds deduplication client limit");
  }
  for (const auto& [key, value] : state.values) {
    if (key.empty() || key.size() > protocol::kMaxKeySize ||
        value.size() > protocol::kMaxValueSize) {
      throw std::invalid_argument("snapshot contains invalid key/value state");
    }
  }
  std::size_t dedup_bytes = 0;
  for (const auto& [client_id, record] : state.clients) {
    const auto decoded = decode_replicated_command(record.command);
    if (!decoded.ok() || decoded.value->client_id != client_id ||
        decoded.value->request_id != record.request_id ||
        encode_replicated_command(*decoded.value) != record.command) {
      throw std::invalid_argument("snapshot contains invalid dedup record");
    }
    validate_response(*decoded.value, record.response);
    const auto record_bytes = record.command.size() + record.response.size();
    if (record_bytes > max_dedup_bytes - dedup_bytes) {
      throw std::invalid_argument("snapshot exceeds deduplication byte limit");
    }
    dedup_bytes += record_bytes;
  }
  return dedup_bytes;
}

}  // namespace

std::size_t ClientIdHash::operator()(const ClientId& id) const noexcept {
  std::uint64_t hash = 0xCBF29CE484222325ULL;
  for (const auto byte : id) {
    hash ^= std::to_integer<std::uint8_t>(byte);
    hash *= 0x100000001B3ULL;
  }
  if constexpr (sizeof(std::size_t) < sizeof(hash)) {
    return static_cast<std::size_t>(hash ^ (hash >> 32U));
  }
  return static_cast<std::size_t>(hash);
}

ReplicatedStateMachine::ReplicatedStateMachine(
    const std::size_t max_clients, const std::size_t max_dedup_bytes)
    : max_clients_(max_clients), max_dedup_bytes_(max_dedup_bytes) {
  if (max_clients == 0 || max_clients > kMaxDedupClients ||
      max_dedup_bytes == 0 || max_dedup_bytes > kMaxDedupBytes) {
    throw std::invalid_argument("invalid deduplication retention limit");
  }
  clients_.reserve(max_clients_);
}

MutationApplyResult ReplicatedStateMachine::apply(
    const ReplicatedCommand& command,
    const std::vector<std::byte>& canonical_command) {
  if (encode_replicated_command(command) != canonical_command) {
    throw std::invalid_argument("noncanonical replicated command");
  }
  const auto retained = clients_.find(command.client_id);
  if (retained != clients_.end()) {
    if (command.request_id < retained->second.request_id) {
      return MutationApplyResult{.status =
                                     MutationApplyStatus::stale_request,
                                 .response = {}};
    }
    if (command.request_id == retained->second.request_id) {
      if (canonical_command != retained->second.command) {
        return MutationApplyResult{
            .status = MutationApplyStatus::request_id_reuse,
            .response = {}};
      }
      return MutationApplyResult{
          .status = MutationApplyStatus::duplicate,
          .response = retained->second.response};
    }
  } else if (clients_.size() >= max_clients_) {
    return MutationApplyResult{
        .status = MutationApplyStatus::capacity_exceeded, .response = {}};
  }

  std::vector<std::byte> response;
  if (command.operation == KvOperation::delete_key) {
    response.push_back(values_.contains(command.key) ? std::byte{1}
                                                     : std::byte{0});
  }
  DedupRecord next{.request_id = command.request_id,
                   .command = canonical_command,
                   .response = response};
  const auto retained_bytes = retained == clients_.end()
                                  ? 0U
                                  : retained->second.command.size() +
                                        retained->second.response.size();
  const auto next_bytes = next.command.size() + next.response.size();
  const auto bytes_without_retained = dedup_bytes_ - retained_bytes;
  if (next_bytes > max_dedup_bytes_ - bytes_without_retained) {
    return MutationApplyResult{
        .status = MutationApplyStatus::capacity_exceeded, .response = {}};
  }

  if (command.operation == KvOperation::put) {
    values_.insert_or_assign(command.key, command.value);
  } else {
    static_cast<void>(values_.erase(command.key));
  }
  if (retained == clients_.end()) {
    clients_.emplace(command.client_id, std::move(next));
  } else {
    retained->second = std::move(next);
  }
  dedup_bytes_ = bytes_without_retained + next_bytes;
  return MutationApplyResult{.status = MutationApplyStatus::applied,
                             .response = std::move(response)};
}

const std::vector<std::byte>* ReplicatedStateMachine::find(
    const std::string_view key) const {
  const auto found = values_.find(std::string{key});
  return found == values_.end() ? nullptr : &found->second;
}

std::size_t ReplicatedStateMachine::client_count() const noexcept {
  return clients_.size();
}

SnapshotState ReplicatedStateMachine::snapshot() const {
  return SnapshotState{.values = values_, .clients = clients_};
}

void ReplicatedStateMachine::restore(SnapshotState state) {
  const auto dedup_bytes =
      validate_snapshot_state(state, max_clients_, max_dedup_bytes_);
  state.clients.reserve(max_clients_);
  values_ = std::move(state.values);
  clients_ = std::move(state.clients);
  dedup_bytes_ = dedup_bytes;
}

}  // namespace forgekv::cluster
