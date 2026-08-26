#pragma once

#include "protocol/frame.h"
#include "raft/types.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace forgekv::cluster {

template <typename Value>
struct DecodeResult final {
  std::optional<Value> value;
  std::string error;

  [[nodiscard]] bool ok() const noexcept { return value.has_value(); }
};

struct PeerEnvelope final {
  std::uint64_t cluster_id{};
  raft::NodeId from{};
  raft::NodeId to{};
  raft::Message message;

  bool operator==(const PeerEnvelope&) const = default;
};

[[nodiscard]] protocol::Frame encode_peer_frame(const PeerEnvelope& envelope,
                                                std::uint64_t request_id);
[[nodiscard]] DecodeResult<PeerEnvelope> decode_peer_frame(
    const protocol::Frame& frame);

enum class KvOperation : std::uint8_t {
  put = 1,
  delete_key = 2,
};

struct ReplicatedCommand final {
  KvOperation operation{KvOperation::put};
  std::array<std::byte, 16> client_id{};
  std::uint64_t request_id{};
  std::string key;
  std::vector<std::byte> value;

  bool operator==(const ReplicatedCommand&) const = default;
};

[[nodiscard]] DecodeResult<ReplicatedCommand> decode_client_mutation(
    const protocol::Frame& frame);
[[nodiscard]] std::vector<std::byte> encode_replicated_command(
    const ReplicatedCommand& command);
[[nodiscard]] DecodeResult<ReplicatedCommand> decode_replicated_command(
    const std::vector<std::byte>& bytes);
[[nodiscard]] DecodeResult<std::string> decode_client_get(
    const protocol::Frame& frame);

}  // namespace forgekv::cluster

