#include "cluster/snapshot_codec.h"

#include "protocol/wire.h"
#include "raft/snapshot_store.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <span>
#include <stdexcept>
#include <string_view>
#include <utility>

namespace forgekv::cluster {
namespace {

namespace wire = forgekv::protocol::wire;

constexpr std::uint32_t kStateMagic = 0x464B5653U;
constexpr std::uint16_t kStateFormatVersion = 1U;
constexpr std::size_t kStateHeaderSize = 16U;
constexpr std::size_t kEntryHeaderSize = 8U;
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

}  // namespace

std::vector<std::byte> encode_snapshot_state(const SnapshotState& state) {
  if (state.size() > kMaxSnapshotKeys) {
    throw std::invalid_argument("snapshot state contains too many keys");
  }
  std::vector<std::pair<std::string_view, const std::vector<std::byte>*>> sorted;
  sorted.reserve(state.size());
  std::size_t size = kStateHeaderSize;
  for (const auto& [key, value] : state) {
    if (key.empty() || key.size() > protocol::kMaxKeySize ||
        value.size() > protocol::kMaxValueSize ||
        size > raft::kMaxSnapshotPayloadSize - kEntryHeaderSize ||
        key.size() >
            raft::kMaxSnapshotPayloadSize - size - kEntryHeaderSize ||
        value.size() > raft::kMaxSnapshotPayloadSize - size -
                           kEntryHeaderSize - key.size()) {
      throw std::invalid_argument("invalid or oversized snapshot state");
    }
    size += kEntryHeaderSize + key.size() + value.size();
    sorted.emplace_back(key, &value);
  }
  std::ranges::sort(sorted, {}, &decltype(sorted)::value_type::first);

  std::vector<std::byte> bytes(size);
  wire::write_u32(std::span{bytes}.subspan(0, 4), kStateMagic);
  write_u16(std::span{bytes}.subspan(4, 2), kStateFormatVersion);
  write_u16(std::span{bytes}.subspan(6, 2),
            static_cast<std::uint16_t>(kStateHeaderSize));
  wire::write_u64(std::span{bytes}.subspan(8, 8), state.size());
  std::size_t offset = kStateHeaderSize;
  for (const auto& [key, value] : sorted) {
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
  return bytes;
}

DecodeResult<SnapshotState> decode_snapshot_state(
    const std::vector<std::byte>& bytes) {
  const auto input = std::span<const std::byte>{bytes};
  if (input.size() < kStateHeaderSize ||
      wire::read_u32(input.subspan(0, 4)) != kStateMagic ||
      read_u16(input.subspan(4, 2)) != kStateFormatVersion ||
      read_u16(input.subspan(6, 2)) != kStateHeaderSize) {
    return failure<SnapshotState>("invalid snapshot state header or version");
  }
  const auto count = wire::read_u64(input.subspan(8, 8));
  if (count > kMaxSnapshotKeys ||
      count > (input.size() - kStateHeaderSize) /
                  (kEntryHeaderSize + 1U)) {
    return failure<SnapshotState>("invalid snapshot state key count");
  }

  SnapshotState state;
  state.reserve(static_cast<std::size_t>(count));
  std::string previous;
  std::size_t offset = kStateHeaderSize;
  for (std::uint64_t index = 0; index < count; ++index) {
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
    const auto key_bytes =
        input.subspan(offset + kEntryHeaderSize, key_size);
    const std::string key(reinterpret_cast<const char*>(key_bytes.data()),
                          key_bytes.size());
    if (!previous.empty() && key <= previous) {
      return failure<SnapshotState>("snapshot keys are not strictly ordered");
    }
    const auto value_bytes = input.subspan(
        offset + kEntryHeaderSize + key_size, value_size);
    state.emplace(key, std::vector<std::byte>(value_bytes.begin(),
                                             value_bytes.end()));
    previous = key;
    offset += kEntryHeaderSize + key_size + value_size;
  }
  if (offset != input.size()) {
    return failure<SnapshotState>("snapshot state has trailing bytes");
  }
  return DecodeResult<SnapshotState>{.value = std::move(state), .error = {}};
}

}  // namespace forgekv::cluster
