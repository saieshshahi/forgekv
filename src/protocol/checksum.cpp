#include "protocol/checksum.h"

#include <array>

namespace forgekv::protocol {
namespace {

constexpr std::array<std::uint32_t, 256> make_crc32_table() {
  std::array<std::uint32_t, 256> table{};
  for (std::uint32_t value = 0; value < table.size(); ++value) {
    std::uint32_t remainder = value;
    for (int bit = 0; bit < 8; ++bit) {
      remainder = (remainder >> 1U) ^
                  ((remainder & 1U) != 0U ? 0xEDB88320U : 0U);
    }
    table[value] = remainder;
  }
  return table;
}

constexpr auto kCrc32Table = make_crc32_table();

}  // namespace

void Crc32::update(const std::span<const std::byte> bytes) noexcept {
  for (const auto byte : bytes) {
    const auto value = std::to_integer<std::uint32_t>(byte);
    const auto index = static_cast<std::size_t>((remainder_ ^ value) & 0xFFU);
    remainder_ = (remainder_ >> 8U) ^ kCrc32Table[index];
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
