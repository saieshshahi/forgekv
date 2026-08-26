#include "cluster/codecs.h"

#include "protocol/wire.h"
#include "raft/raft_storage.h"

#include <algorithm>
#include <limits>
#include <span>
#include <stdexcept>
#include <type_traits>

namespace forgekv::cluster {
namespace {

namespace wire = forgekv::protocol::wire;

constexpr std::size_t kPeerCommonSize = 32U;
constexpr std::size_t kEntryHeaderSize = 24U;
constexpr std::size_t kCommandHeaderSize = 40U;

enum class PeerKind : std::uint8_t {
  request_vote = 1,
  request_vote_response = 2,
  append_entries = 3,
  append_entries_response = 4,
};

template <typename Value>
DecodeResult<Value> failure(std::string message) {
  return DecodeResult<Value>{.value = std::nullopt,
                             .error = std::move(message)};
}

bool all_zero(const std::span<const std::byte> bytes) {
  return std::ranges::all_of(bytes,
                             [](const std::byte byte) { return byte == std::byte{0}; });
}

protocol::MessageType peer_type(const raft::Message& message) {
  return std::visit(
      [](const auto& typed) {
        using Type = std::decay_t<decltype(typed)>;
        if constexpr (std::is_same_v<Type, raft::RequestVote> ||
                      std::is_same_v<Type, raft::RequestVoteResponse>) {
          return protocol::MessageType::raft_request_vote;
        }
        return protocol::MessageType::raft_append_entries;
      },
      message);
}

PeerKind peer_kind(const raft::Message& message) {
  return std::visit(
      [](const auto& typed) {
        using Type = std::decay_t<decltype(typed)>;
        if constexpr (std::is_same_v<Type, raft::RequestVote>) {
          return PeerKind::request_vote;
        } else if constexpr (std::is_same_v<Type,
                                            raft::RequestVoteResponse>) {
          return PeerKind::request_vote_response;
        } else if constexpr (std::is_same_v<Type, raft::AppendEntries>) {
          return PeerKind::append_entries;
        } else {
          return PeerKind::append_entries_response;
        }
      },
      message);
}

std::size_t peer_payload_size(const raft::Message& message) {
  return std::visit(
      [](const auto& typed) -> std::size_t {
        using Type = std::decay_t<decltype(typed)>;
        if constexpr (std::is_same_v<Type, raft::RequestVote>) {
          return kPeerCommonSize + 32U;
        } else if constexpr (std::is_same_v<Type,
                                            raft::RequestVoteResponse>) {
          return kPeerCommonSize + 16U;
        } else if constexpr (std::is_same_v<Type, raft::AppendEntries>) {
          if (typed.rpc_id == 0 ||
              typed.entries.size() > raft::kMaxRaftLogEntriesPerRecord) {
            throw std::invalid_argument("invalid Raft AppendEntries batch");
          }
          std::size_t size = kPeerCommonSize + 56U;
          for (const auto& entry : typed.entries) {
            if (entry.index == 0 || entry.term == 0 ||
                entry.command.size() > raft::kMaxRaftCommandSize ||
                (entry.kind == raft::EntryKind::no_op &&
                 !entry.command.empty()) ||
                (entry.kind != raft::EntryKind::command &&
                 entry.kind != raft::EntryKind::no_op) ||
                size > protocol::kMaxPayloadSize - kEntryHeaderSize ||
                entry.command.size() >
                    protocol::kMaxPayloadSize - size - kEntryHeaderSize) {
              throw std::invalid_argument("Raft AppendEntries exceeds frame limit");
            }
            size += kEntryHeaderSize + entry.command.size();
          }
          return size;
        } else {
          return kPeerCommonSize + 40U;
        }
      },
      message);
}

bool valid_key_size(const std::size_t size) {
  return size > 0 && size <= protocol::kMaxKeySize;
}

}  // namespace

protocol::Frame encode_peer_frame(const PeerEnvelope& envelope,
                                  const std::uint64_t request_id) {
  if (envelope.cluster_id == 0 || envelope.from == 0 || envelope.to == 0) {
    throw std::invalid_argument("Raft peer envelope identities must be nonzero");
  }
  const auto size = peer_payload_size(envelope.message);
  if (size > protocol::kMaxPayloadSize) {
    throw std::invalid_argument("Raft peer payload exceeds frame limit");
  }
  std::vector<std::byte> payload(size);
  wire::write_u64(std::span{payload}.subspan(0, 8), envelope.cluster_id);
  wire::write_u64(std::span{payload}.subspan(8, 8), envelope.from);
  wire::write_u64(std::span{payload}.subspan(16, 8), envelope.to);
  payload[24] = static_cast<std::byte>(peer_kind(envelope.message));

  std::visit(
      [&payload](const auto& message) {
        using Type = std::decay_t<decltype(message)>;
        auto body = std::span{payload}.subspan(kPeerCommonSize);
        if constexpr (std::is_same_v<Type, raft::RequestVote>) {
          wire::write_u64(body.subspan(0, 8), message.term);
          wire::write_u64(body.subspan(8, 8), message.candidate_id);
          wire::write_u64(body.subspan(16, 8), message.last_log_index);
          wire::write_u64(body.subspan(24, 8), message.last_log_term);
        } else if constexpr (std::is_same_v<Type,
                                            raft::RequestVoteResponse>) {
          wire::write_u64(body.subspan(0, 8), message.term);
          body[8] = message.vote_granted ? std::byte{1} : std::byte{0};
        } else if constexpr (std::is_same_v<Type, raft::AppendEntries>) {
          wire::write_u64(body.subspan(0, 8), message.term);
          wire::write_u64(body.subspan(8, 8), message.leader_id);
          wire::write_u64(body.subspan(16, 8), message.previous_log_index);
          wire::write_u64(body.subspan(24, 8), message.previous_log_term);
          wire::write_u64(body.subspan(32, 8), message.leader_commit);
          wire::write_u64(body.subspan(40, 8), message.rpc_id);
          if (message.entries.size() >
              std::numeric_limits<std::uint32_t>::max()) {
            throw std::invalid_argument("too many Raft entries");
          }
          wire::write_u32(body.subspan(48, 4),
                          static_cast<std::uint32_t>(message.entries.size()));
          std::size_t offset = 56U;
          for (const auto& entry : message.entries) {
            wire::write_u64(body.subspan(offset, 8), entry.index);
            wire::write_u64(body.subspan(offset + 8, 8), entry.term);
            body[offset + 16] = static_cast<std::byte>(entry.kind);
            wire::write_u32(body.subspan(offset + 20, 4),
                            static_cast<std::uint32_t>(entry.command.size()));
            std::ranges::copy(
                entry.command,
                body.begin() + static_cast<std::ptrdiff_t>(offset + 24U));
            offset += kEntryHeaderSize + entry.command.size();
          }
        } else {
          wire::write_u64(body.subspan(0, 8), message.term);
          body[8] = message.success ? std::byte{1} : std::byte{0};
          wire::write_u64(body.subspan(16, 8), message.match_index);
          wire::write_u64(body.subspan(24, 8), message.reject_hint);
          wire::write_u64(body.subspan(32, 8), message.rpc_id);
        }
      },
      envelope.message);

  return protocol::Frame{
      .message_namespace = protocol::Namespace::raft,
      .message_type = peer_type(envelope.message),
      .flags = 0,
      .request_id = request_id,
      .payload = std::move(payload),
  };
}

DecodeResult<PeerEnvelope> decode_peer_frame(const protocol::Frame& frame) {
  if (frame.message_namespace != protocol::Namespace::raft ||
      frame.payload.size() < kPeerCommonSize ||
      !all_zero(std::span{frame.payload}.subspan(25, 7))) {
    return failure<PeerEnvelope>("invalid Raft peer envelope");
  }
  const auto payload = std::span<const std::byte>{frame.payload};
  PeerEnvelope envelope{
      .cluster_id = wire::read_u64(payload.subspan(0, 8)),
      .from = wire::read_u64(payload.subspan(8, 8)),
      .to = wire::read_u64(payload.subspan(16, 8)),
      .message = raft::RequestVote{},
  };
  if (envelope.cluster_id == 0 || envelope.from == 0 || envelope.to == 0) {
    return failure<PeerEnvelope>("zero Raft peer identity");
  }
  const auto kind = static_cast<PeerKind>(
      std::to_integer<std::uint8_t>(payload[24]));
  const auto body = payload.subspan(kPeerCommonSize);
  if (kind == PeerKind::request_vote) {
    if (frame.message_type != protocol::MessageType::raft_request_vote ||
        body.size() != 32U) {
      return failure<PeerEnvelope>("invalid RequestVote payload size");
    }
    envelope.message = raft::RequestVote{
        .term = wire::read_u64(body.subspan(0, 8)),
        .candidate_id = wire::read_u64(body.subspan(8, 8)),
        .last_log_index = wire::read_u64(body.subspan(16, 8)),
        .last_log_term = wire::read_u64(body.subspan(24, 8)),
    };
  } else if (kind == PeerKind::request_vote_response) {
    if (frame.message_type != protocol::MessageType::raft_request_vote ||
        body.size() != 16U || body[8] > std::byte{1} ||
        !all_zero(body.subspan(9, 7))) {
      return failure<PeerEnvelope>("invalid RequestVoteResponse payload");
    }
    envelope.message = raft::RequestVoteResponse{
        .term = wire::read_u64(body.subspan(0, 8)),
        .vote_granted = body[8] == std::byte{1},
    };
  } else if (kind == PeerKind::append_entries) {
    if (frame.message_type != protocol::MessageType::raft_append_entries ||
        body.size() < 56U || !all_zero(body.subspan(52, 4))) {
      return failure<PeerEnvelope>("invalid AppendEntries header");
    }
    const auto count = wire::read_u32(body.subspan(48, 4));
    const auto maximum_entries =
        (body.size() - 56U) / kEntryHeaderSize;
    if (count > maximum_entries ||
        count > raft::kMaxRaftLogEntriesPerRecord) {
      return failure<PeerEnvelope>("impossible AppendEntries count");
    }
    raft::AppendEntries append{
        .term = wire::read_u64(body.subspan(0, 8)),
        .leader_id = wire::read_u64(body.subspan(8, 8)),
        .previous_log_index = wire::read_u64(body.subspan(16, 8)),
        .previous_log_term = wire::read_u64(body.subspan(24, 8)),
        .entries = {},
        .leader_commit = wire::read_u64(body.subspan(32, 8)),
        .rpc_id = wire::read_u64(body.subspan(40, 8)),
    };
    append.entries.reserve(count);
    std::size_t offset = 56U;
    for (std::uint32_t index = 0; index < count; ++index) {
      if (body.size() - offset < kEntryHeaderSize) {
        return failure<PeerEnvelope>("truncated Raft entry header");
      }
      const auto command_size = wire::read_u32(body.subspan(offset + 20, 4));
      if (command_size > raft::kMaxRaftCommandSize ||
          command_size > body.size() - offset - kEntryHeaderSize ||
          !all_zero(body.subspan(offset + 17, 3))) {
        return failure<PeerEnvelope>("invalid Raft entry length");
      }
      const auto entry_kind = static_cast<raft::EntryKind>(
          std::to_integer<std::uint8_t>(body[offset + 16]));
      if (entry_kind != raft::EntryKind::command &&
          entry_kind != raft::EntryKind::no_op) {
        return failure<PeerEnvelope>("invalid Raft entry kind");
      }
      const auto entry_index = wire::read_u64(body.subspan(offset, 8));
      const auto entry_term = wire::read_u64(body.subspan(offset + 8, 8));
      if (entry_index == 0 || entry_term == 0 ||
          (entry_kind == raft::EntryKind::no_op && command_size != 0)) {
        return failure<PeerEnvelope>("invalid Raft entry semantics");
      }
      const auto command =
          body.subspan(offset + kEntryHeaderSize, command_size);
      append.entries.push_back(raft::LogEntry{
          .index = entry_index,
          .term = entry_term,
          .kind = entry_kind,
          .command = std::vector<std::byte>(command.begin(), command.end()),
      });
      offset += kEntryHeaderSize + command_size;
    }
    if (offset != body.size()) {
      return failure<PeerEnvelope>("trailing AppendEntries bytes");
    }
    envelope.message = std::move(append);
  } else if (kind == PeerKind::append_entries_response) {
    if (frame.message_type != protocol::MessageType::raft_append_entries ||
        body.size() != 40U || body[8] > std::byte{1} ||
        !all_zero(body.subspan(9, 7))) {
      return failure<PeerEnvelope>("invalid AppendEntriesResponse payload");
    }
    envelope.message = raft::AppendEntriesResponse{
        .term = wire::read_u64(body.subspan(0, 8)),
        .success = body[8] == std::byte{1},
        .match_index = wire::read_u64(body.subspan(16, 8)),
        .reject_hint = wire::read_u64(body.subspan(24, 8)),
        .rpc_id = wire::read_u64(body.subspan(32, 8)),
    };
  } else {
    return failure<PeerEnvelope>("unknown Raft peer message kind");
  }
  return DecodeResult<PeerEnvelope>{.value = std::move(envelope), .error = {}};
}

DecodeResult<ReplicatedCommand> decode_client_mutation(
    const protocol::Frame& frame) {
  if (frame.message_namespace != protocol::Namespace::client ||
      (frame.message_type != protocol::MessageType::put &&
       frame.message_type != protocol::MessageType::delete_key) ||
      frame.request_id == 0 || frame.payload.size() < 20U) {
    return failure<ReplicatedCommand>("invalid mutation frame");
  }
  const auto payload = std::span<const std::byte>{frame.payload};
  ReplicatedCommand command;
  command.operation = frame.message_type == protocol::MessageType::put
                          ? KvOperation::put
                          : KvOperation::delete_key;
  std::ranges::copy(payload.first<16>(), command.client_id.begin());
  command.request_id = frame.request_id;
  const auto key_size = wire::read_u32(payload.subspan(16, 4));
  std::uint32_t value_size = 0;
  std::size_t header_size = 20U;
  if (command.operation == KvOperation::put) {
    if (payload.size() < 24U) {
      return failure<ReplicatedCommand>("truncated PUT payload");
    }
    value_size = wire::read_u32(payload.subspan(20, 4));
    header_size = 24U;
  }
  if (!valid_key_size(key_size) || value_size > protocol::kMaxValueSize ||
      payload.size() != header_size + key_size + value_size) {
    return failure<ReplicatedCommand>("invalid mutation key/value lengths");
  }
  const auto key = payload.subspan(header_size, key_size);
  command.key.assign(reinterpret_cast<const char*>(key.data()), key.size());
  const auto value = payload.subspan(header_size + key_size, value_size);
  command.value.assign(value.begin(), value.end());
  return DecodeResult<ReplicatedCommand>{.value = std::move(command),
                                         .error = {}};
}

std::vector<std::byte> encode_replicated_command(
    const ReplicatedCommand& command) {
  if (!valid_key_size(command.key.size()) ||
      command.value.size() > protocol::kMaxValueSize ||
      (command.operation == KvOperation::delete_key &&
       !command.value.empty()) ||
      command.request_id == 0) {
    throw std::invalid_argument("invalid replicated KV command");
  }
  std::vector<std::byte> bytes(kCommandHeaderSize + command.key.size() +
                               command.value.size());
  bytes[0] = static_cast<std::byte>(command.operation);
  std::ranges::copy(command.client_id, bytes.begin() + 8);
  wire::write_u64(std::span{bytes}.subspan(24, 8), command.request_id);
  wire::write_u32(std::span{bytes}.subspan(32, 4),
                  static_cast<std::uint32_t>(command.key.size()));
  wire::write_u32(std::span{bytes}.subspan(36, 4),
                  static_cast<std::uint32_t>(command.value.size()));
  std::ranges::transform(
      command.key,
      bytes.begin() + static_cast<std::ptrdiff_t>(kCommandHeaderSize),
      [](const char character) {
        return static_cast<std::byte>(static_cast<unsigned char>(character));
      });
  std::ranges::copy(
      command.value,
      bytes.begin() + static_cast<std::ptrdiff_t>(kCommandHeaderSize +
                                                  command.key.size()));
  return bytes;
}

DecodeResult<ReplicatedCommand> decode_replicated_command(
    const std::vector<std::byte>& bytes) {
  if (bytes.size() < kCommandHeaderSize ||
      !all_zero(std::span{bytes}.subspan(1, 7))) {
    return failure<ReplicatedCommand>("invalid replicated command header");
  }
  const auto operation = static_cast<KvOperation>(
      std::to_integer<std::uint8_t>(bytes[0]));
  const auto source = std::span<const std::byte>{bytes};
  const auto key_size = wire::read_u32(source.subspan(32, 4));
  const auto value_size = wire::read_u32(source.subspan(36, 4));
  if ((operation != KvOperation::put && operation != KvOperation::delete_key) ||
      !valid_key_size(key_size) || value_size > protocol::kMaxValueSize ||
      (operation == KvOperation::delete_key && value_size != 0) ||
      source.size() != kCommandHeaderSize + key_size + value_size) {
    return failure<ReplicatedCommand>("invalid replicated command lengths");
  }
  ReplicatedCommand command{.operation = operation,
                            .client_id = {},
                            .request_id = 0,
                            .key = {},
                            .value = {}};
  std::ranges::copy(source.subspan(8, 16), command.client_id.begin());
  command.request_id = wire::read_u64(source.subspan(24, 8));
  if (command.request_id == 0) {
    return failure<ReplicatedCommand>("zero replicated request ID");
  }
  const auto key = source.subspan(kCommandHeaderSize, key_size);
  command.key.assign(reinterpret_cast<const char*>(key.data()), key.size());
  const auto value = source.subspan(kCommandHeaderSize + key_size, value_size);
  command.value.assign(value.begin(), value.end());
  return DecodeResult<ReplicatedCommand>{.value = std::move(command),
                                         .error = {}};
}

DecodeResult<std::string> decode_client_get(const protocol::Frame& frame) {
  if (frame.message_namespace != protocol::Namespace::client ||
      frame.message_type != protocol::MessageType::get ||
      frame.payload.size() < 4U) {
    return failure<std::string>("invalid GET frame");
  }
  const auto payload = std::span<const std::byte>{frame.payload};
  const auto key_size = wire::read_u32(payload.first<4>());
  if (!valid_key_size(key_size) || payload.size() != 4U + key_size) {
    return failure<std::string>("invalid GET key length");
  }
  const auto key = payload.subspan(4, key_size);
  return DecodeResult<std::string>{
      .value = std::string(reinterpret_cast<const char*>(key.data()), key.size()),
      .error = {},
  };
}

}  // namespace forgekv::cluster
