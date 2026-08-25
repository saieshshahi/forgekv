#include "protocol/checksum.h"

#include <array>

namespace forgekv::protocol {
namespace {

using Crc32Table = std::array<std::uint32_t, 256>;
using Crc32Tables = std::array<Crc32Table, 8>;

constexpr Crc32Tables make_crc32_tables() {
  Crc32Tables tables{};
  for (std::uint32_t value = 0; value < tables.front().size(); ++value) {
    std::uint32_t remainder = value;
    for (int bit = 0; bit < 8; ++bit) {
      remainder = (remainder >> 1U) ^
                  ((remainder & 1U) != 0U ? 0xEDB88320U : 0U);
    }
    tables[0][value] = remainder;
  }

  for (std::size_t slice = 1; slice < tables.size(); ++slice) {
    for (std::size_t value = 0; value < tables[slice].size(); ++value) {
      const auto previous = tables[slice - 1][value];
      tables[slice][value] =
          (previous >> 8U) ^ tables[0][previous & 0xFFU];
    }
  }
  return tables;
}

constexpr auto kCrc32Tables = make_crc32_tables();

}  // namespace

void Crc32::update(const std::span<const std::byte> bytes) noexcept {
  auto remaining = bytes;
  while (remaining.size() >= 8) {
    const auto first = std::to_integer<std::uint32_t>(remaining[0]) |
                       (std::to_integer<std::uint32_t>(remaining[1]) << 8U) |
                       (std::to_integer<std::uint32_t>(remaining[2]) << 16U) |
                       (std::to_integer<std::uint32_t>(remaining[3]) << 24U);
    const auto combined = remainder_ ^ first;
    remainder_ =
        kCrc32Tables[7][combined & 0xFFU] ^
        kCrc32Tables[6][(combined >> 8U) & 0xFFU] ^
        kCrc32Tables[5][(combined >> 16U) & 0xFFU] ^
        kCrc32Tables[4][combined >> 24U] ^
        kCrc32Tables[3][std::to_integer<std::uint8_t>(remaining[4])] ^
        kCrc32Tables[2][std::to_integer<std::uint8_t>(remaining[5])] ^
        kCrc32Tables[1][std::to_integer<std::uint8_t>(remaining[6])] ^
        kCrc32Tables[0][std::to_integer<std::uint8_t>(remaining[7])];
    remaining = remaining.subspan(8);
  }

  for (const auto byte : remaining) {
    const auto value = std::to_integer<std::uint32_t>(byte);
    const auto index = static_cast<std::size_t>((remainder_ ^ value) & 0xFFU);
    remainder_ = (remainder_ >> 8U) ^ kCrc32Tables[0][index];
  }
}

std::uint32_t Crc32::value() const noexcept {
  return remainder_ ^ 0xFFFFFFFFU;
}

std::uint32_t crc32(const std::span<const std::byte> bytes) noexcept {
  Crc32 checksum;
  checksum.update(bytes);
  return checksum.value();
}

}  // namespace forgekv::protocol
