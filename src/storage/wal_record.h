#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace forgekv::storage {

inline constexpr std::uint32_t kWalMagic = 0x4657414CU;
inline constexpr std::uint16_t kWalVersion = 1U;
inline constexpr std::size_t kWalHeaderSize = 36U;
inline constexpr std::size_t kMaxKeySize = 1024U;
inline constexpr std::size_t kMaxValueSize = 1024U * 1024U;
inline constexpr std::size_t kMaxWalRecordSize =
    kWalHeaderSize + kMaxKeySize + kMaxValueSize;

enum class WalOperation : std::uint8_t {
  put = 1U,
  delete_key = 2U,
};

struct WalRecord {
  std::uint64_t lsn{0U};
  WalOperation operation{WalOperation::put};
  std::string key;
  std::string value;

  bool operator==(const WalRecord&) const = default;
};

enum class DecodeStatus {
  complete,
  incomplete,
  corrupt,
};

struct DecodeResult {
  DecodeStatus status{DecodeStatus::incomplete};
  std::optional<WalRecord> record;
  std::size_t consumed{0U};
  std::string error;
};

[[nodiscard]] std::vector<std::byte> encode_record(const WalRecord& record);
[[nodiscard]] DecodeResult decode_record(std::span<const std::byte> bytes);

}  // namespace forgekv::storage
