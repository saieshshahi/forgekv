#include "cluster/snapshot_codec.h"

#include "protocol/wire.h"
#include "raft/snapshot_store.h"

#include <algorithm>
#include <cstdint>
#include <optional>
#include <span>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <variant>

namespace forgekv::cluster {
namespace {

namespace wire = forgekv::protocol::wire;

constexpr std::uint32_t kStateMagic = 0x464B5653U;
constexpr std::uint16_t kLegacyStateFormatVersion = 1U;
constexpr std::uint16_t kStateFormatVersion = 2U;
constexpr std::size_t kLegacyStateHeaderSize = 16U;
constexpr std::size_t kStateHeaderSize = 24U;
constexpr std::size_t kEntryHeaderSize = 8U;
constexpr std::size_t kDedupEntryHeaderSize = 32U;
constexpr std::size_t kMaxSnapshotKeys = 1U << 20U;

void write_u16(const std::span<std::byte> destination,
               const std::uint16_t value) {
  destination[0] = static_cast<std::byte>((value >> 8U) & 0xFFU);
  destination[1] = static_cast<std::byte>(value & 0xFFU);
}

std::uint16_t read_u16(const std::span<const std::byte> source) {
  return static_cast<std::uint16_t>(
      (std::to_integer<std::uint16_t>(source[0]) << 8U) |
      std::to_integer<std::uint16_t>(source[1]));
}

template <typename Value>
DecodeResult<Value> failure(std::string message) {
  return DecodeResult<Value>{.value = std::nullopt,
                             .error = std::move(message)};
}

void add_size(std::size_t& total, const std::size_t amount) {
  if (amount > raft::kMaxSnapshotPayloadSize - total) {
    throw std::invalid_argument("snapshot state exceeds payload limit");
  }
  total += amount;
}

bool valid_stored_result(const ReplicatedCommand& command,
                         const std::vector<std::byte>& response) {
  if (command.operation == KvOperation::put) {
    return response.empty();
  }
  return response.size() == 1U &&
         (response[0] == std::byte{0} || response[0] == std::byte{1});
}

bool client_less(const ClientId& left, const ClientId& right) {
  return std::ranges::lexicographical_compare(left, right);
}

DecodeResult<std::monostate> validate_record(const ClientId& client_id,
                                             const DedupRecord& record) {
  if (record.request_id == 0 ||
      record.command.size() > protocol::kMaxClientCommandSize) {
    return failure<std::monostate>("invalid snapshot dedup record length");
  }
  const auto command = decode_replicated_command(record.command);
  if (!command.ok() || command.value->client_id != client_id ||
      command.value->request_id != record.request_id ||
      encode_replicated_command(*command.value) != record.command ||
      !valid_stored_result(*command.value, record.response)) {
    return failure<std::monostate>("invalid snapshot dedup record");
  }
  return DecodeResult<std::monostate>{.value = std::monostate{}, .error = {}};
}

}  // namespace

std::vector<std::byte> encode_snapshot_state(const SnapshotState& state) {
  if (state.values.size() > kMaxSnapshotKeys ||
      state.clients.size() > kMaxDedupClients) {
    throw std::invalid_argument("snapshot state exceeds entry limits");
  }

  std::vector<std::pair<std::string_view, const std::vector<std::byte>*>>
      sorted_values;
  sorted_values.reserve(state.values.size());
  std::size_t size = kStateHeaderSize;
  for (const auto& [key, value] : state.values) {
    if (key.empty() || key.size() > protocol::kMaxKeySize ||
        value.size() > protocol::kMaxValueSize) {
      throw std::invalid_argument("invalid snapshot key/value state");
    }
    add_size(size, kEntryHeaderSize);
    add_size(size, key.size());
    add_size(size, value.size());
    sorted_values.emplace_back(key, &value);
  }
  std::ranges::sort(sorted_values, {},
                    &decltype(sorted_values)::value_type::first);

  std::vector<std::pair<ClientId, const DedupRecord*>> sorted_clients;
  sorted_clients.reserve(state.clients.size());
  std::size_t dedup_bytes = 0;
  for (const auto& [client_id, record] : state.clients) {
    const auto valid = validate_record(client_id, record);
    if (!valid.ok()) {
      throw std::invalid_argument(valid.error);
    }
    const auto record_bytes = record.command.size() + record.response.size();
    if (record_bytes > kMaxDedupBytes - dedup_bytes) {
      throw std::invalid_argument("snapshot exceeds deduplication byte limit");
    }
    dedup_bytes += record_bytes;
    add_size(size, kDedupEntryHeaderSize);
    add_size(size, record.command.size());
    add_size(size, record.response.size());
    sorted_clients.emplace_back(client_id, &record);
  }
  std::ranges::sort(sorted_clients, [](const auto& left, const auto& right) {
    return client_less(left.first, right.first);
  });

  std::vector<std::byte> bytes(size);
  wire::write_u32(std::span{bytes}.subspan(0, 4), kStateMagic);
  write_u16(std::span{bytes}.subspan(4, 2), kStateFormatVersion);
  write_u16(std::span{bytes}.subspan(6, 2),
            static_cast<std::uint16_t>(kStateHeaderSize));
  wire::write_u64(std::span{bytes}.subspan(8, 8), state.values.size());
  wire::write_u64(std::span{bytes}.subspan(16, 8), state.clients.size());

  std::size_t offset = kStateHeaderSize;
  for (const auto& [key, value] : sorted_values) {
    wire::write_u32(std::span{bytes}.subspan(offset, 4),
                    static_cast<std::uint32_t>(key.size()));
    wire::write_u32(std::span{bytes}.subspan(offset + 4U, 4),
                    static_cast<std::uint32_t>(value->size()));
    std::ranges::transform(
        key, bytes.begin() + static_cast<std::ptrdiff_t>(offset + 8U),
        [](const char character) {
          return static_cast<std::byte>(static_cast<unsigned char>(character));
        });
    std::ranges::copy(
        *value, bytes.begin() + static_cast<std::ptrdiff_t>(
                                    offset + kEntryHeaderSize + key.size()));
    offset += kEntryHeaderSize + key.size() + value->size();
  }
  for (const auto& [client_id, record] : sorted_clients) {
    std::ranges::copy(client_id,
                      bytes.begin() + static_cast<std::ptrdiff_t>(offset));
    wire::write_u64(std::span{bytes}.subspan(offset + 16U, 8),
                    record->request_id);
    wire::write_u32(std::span{bytes}.subspan(offset + 24U, 4),
                    static_cast<std::uint32_t>(record->command.size()));
    wire::write_u32(std::span{bytes}.subspan(offset + 28U, 4),
                    static_cast<std::uint32_t>(record->response.size()));
    std::ranges::copy(
        record->command,
        bytes.begin() + static_cast<std::ptrdiff_t>(offset + 32U));
    std::ranges::copy(
        record->response,
        bytes.begin() + static_cast<std::ptrdiff_t>(
                            offset + 32U + record->command.size()));
    offset += kDedupEntryHeaderSize + record->command.size() +
              record->response.size();
  }
  return bytes;
}

DecodeResult<SnapshotState> decode_snapshot_state(
    const std::vector<std::byte>& bytes) {
  const auto input = std::span<const std::byte>{bytes};
  if (input.size() < kLegacyStateHeaderSize ||
      wire::read_u32(input.subspan(0, 4)) != kStateMagic) {
    return failure<SnapshotState>("invalid snapshot state header");
  }
  const auto version = read_u16(input.subspan(4, 2));
  const auto header_size = read_u16(input.subspan(6, 2));
  if ((version == kLegacyStateFormatVersion &&
       header_size != kLegacyStateHeaderSize) ||
      (version == kStateFormatVersion && header_size != kStateHeaderSize) ||
      (version != kLegacyStateFormatVersion &&
       version != kStateFormatVersion) ||
      input.size() < header_size) {
    return failure<SnapshotState>("invalid snapshot state version");
  }

  const auto key_count = wire::read_u64(input.subspan(8, 8));
  const auto client_count = version == kStateFormatVersion
                                ? wire::read_u64(input.subspan(16, 8))
                                : 0U;
  if (key_count > kMaxSnapshotKeys || client_count > kMaxDedupClients ||
      key_count > (input.size() - header_size) / (kEntryHeaderSize + 1U)) {
    return failure<SnapshotState>("invalid snapshot state entry count");
  }

  SnapshotState state;
  state.values.reserve(static_cast<std::size_t>(key_count));
  state.clients.reserve(static_cast<std::size_t>(client_count));
  std::string previous_key;
  std::size_t offset = header_size;
  for (std::uint64_t index = 0; index < key_count; ++index) {
    if (input.size() - offset < kEntryHeaderSize) {
      return failure<SnapshotState>("truncated snapshot state entry");
    }
    const auto key_size = wire::read_u32(input.subspan(offset, 4));
    const auto value_size = wire::read_u32(input.subspan(offset + 4U, 4));
    if (key_size == 0 || key_size > protocol::kMaxKeySize ||
        value_size > protocol::kMaxValueSize ||
        key_size > input.size() - offset - kEntryHeaderSize ||
        value_size > input.size() - offset - kEntryHeaderSize - key_size) {
      return failure<SnapshotState>("invalid snapshot key/value length");
    }
    const auto key_bytes = input.subspan(offset + kEntryHeaderSize, key_size);
    const std::string key(reinterpret_cast<const char*>(key_bytes.data()),
                          key_bytes.size());
    if (!previous_key.empty() && key <= previous_key) {
      return failure<SnapshotState>("snapshot keys are not strictly ordered");
    }
    const auto value_bytes = input.subspan(
        offset + kEntryHeaderSize + key_size, value_size);
    state.values.emplace(key, std::vector<std::byte>(value_bytes.begin(),
                                                    value_bytes.end()));
    previous_key = key;
    offset += kEntryHeaderSize + key_size + value_size;
  }

  std::optional<ClientId> previous_client;
  std::size_t decoded_dedup_bytes = 0;
  for (std::uint64_t index = 0; index < client_count; ++index) {
    if (input.size() - offset < kDedupEntryHeaderSize) {
      return failure<SnapshotState>("truncated snapshot dedup entry");
    }
    ClientId client_id{};
    std::ranges::copy(input.subspan(offset, 16), client_id.begin());
    const auto request_id = wire::read_u64(input.subspan(offset + 16U, 8));
    const auto command_size = wire::read_u32(input.subspan(offset + 24U, 4));
    const auto response_size = wire::read_u32(input.subspan(offset + 28U, 4));
    if (previous_client.has_value() &&
        !client_less(*previous_client, client_id)) {
      return failure<SnapshotState>(
          "snapshot clients are not strictly ordered");
    }
    if (request_id == 0 || command_size > protocol::kMaxClientCommandSize ||
        response_size > 1U ||
        command_size > input.size() - offset - kDedupEntryHeaderSize ||
        response_size > input.size() - offset - kDedupEntryHeaderSize -
                            command_size) {
      return failure<SnapshotState>("invalid snapshot dedup length");
    }
    const auto record_bytes =
        static_cast<std::size_t>(command_size) + response_size;
    if (record_bytes > kMaxDedupBytes - decoded_dedup_bytes) {
      return failure<SnapshotState>(
          "snapshot exceeds deduplication byte limit");
    }
    decoded_dedup_bytes += record_bytes;
    const auto command_bytes = input.subspan(offset + kDedupEntryHeaderSize,
                                             command_size);
    const auto response_bytes = input.subspan(
        offset + kDedupEntryHeaderSize + command_size, response_size);
    DedupRecord record{
        .request_id = request_id,
        .command = {command_bytes.begin(), command_bytes.end()},
        .response = {response_bytes.begin(), response_bytes.end()},
    };
    const auto valid = validate_record(client_id, record);
    if (!valid.ok()) {
      return failure<SnapshotState>(valid.error);
    }
    state.clients.emplace(client_id, std::move(record));
    previous_client = client_id;
    offset += kDedupEntryHeaderSize + command_size + response_size;
  }
  if (offset != input.size()) {
    return failure<SnapshotState>("snapshot state has trailing bytes");
  }
  return DecodeResult<SnapshotState>{.value = std::move(state), .error = {}};
}

}  // namespace forgekv::cluster
