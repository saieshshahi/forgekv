#pragma once

#include "cluster/codecs.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace forgekv::cluster {

using ClientId = std::array<std::byte, 16>;

struct ClientIdHash final {
  [[nodiscard]] std::size_t operator()(const ClientId& id) const noexcept;
};

struct DedupRecord final {
  std::uint64_t request_id{};
  std::vector<std::byte> command;
  std::vector<std::byte> response;

  bool operator==(const DedupRecord&) const = default;
};

using KeyValueState =
    std::unordered_map<std::string, std::vector<std::byte>>;
using DedupState =
    std::unordered_map<ClientId, DedupRecord, ClientIdHash>;

struct SnapshotState final {
  KeyValueState values;
  DedupState clients;

  bool operator==(const SnapshotState&) const = default;
};

enum class MutationApplyStatus {
  applied,
  duplicate,
  request_id_reuse,
  stale_request,
  capacity_exceeded,
};

struct MutationApplyResult final {
  MutationApplyStatus status{MutationApplyStatus::applied};
  std::vector<std::byte> response;

  bool operator==(const MutationApplyResult&) const = default;
};

inline constexpr std::size_t kMaxDedupClients = 1024U;
inline constexpr std::size_t kMaxDedupBytes = 64U * 1024U * 1024U;

class ReplicatedStateMachine final {
 public:
  explicit ReplicatedStateMachine(
      std::size_t max_clients = kMaxDedupClients,
      std::size_t max_dedup_bytes = kMaxDedupBytes);

  [[nodiscard]] MutationApplyResult apply(
      const ReplicatedCommand& command,
      const std::vector<std::byte>& canonical_command);
  [[nodiscard]] const std::vector<std::byte>* find(
      std::string_view key) const;
  [[nodiscard]] std::size_t client_count() const noexcept;
  [[nodiscard]] SnapshotState snapshot() const;
  void restore(SnapshotState state);

 private:
  std::size_t max_clients_{};
  std::size_t max_dedup_bytes_{};
  std::size_t dedup_bytes_{};
  KeyValueState values_;
  DedupState clients_;
};

}  // namespace forgekv::cluster
