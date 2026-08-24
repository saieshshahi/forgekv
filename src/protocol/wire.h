#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

namespace forgekv::protocol::wire {

inline void write_u32(std::span<std::byte> destination, const std::uint32_t value) {
  destination[0] = static_cast<std::byte>((value >> 24U) & 0xFFU);
  destination[1] = static_cast<std::byte>((value >> 16U) & 0xFFU);
  destination[2] = static_cast<std::byte>((value >> 8U) & 0xFFU);
  destination[3] = static_cast<std::byte>(value & 0xFFU);
}

inline void write_u64(std::span<std::byte> destination, const std::uint64_t value) {
  for (std::size_t index = 0; index < 8U; ++index) {
    const auto shift = static_cast<unsigned int>((7U - index) * 8U);
    destination[index] = static_cast<std::byte>((value >> shift) & 0xFFU);
  }
}

[[nodiscard]] inline std::uint32_t read_u32(const std::span<const std::byte> source) {
  return (std::to_integer<std::uint32_t>(source[0]) << 24U) |
         (std::to_integer<std::uint32_t>(source[1]) << 16U) |
         (std::to_integer<std::uint32_t>(source[2]) << 8U) |
         std::to_integer<std::uint32_t>(source[3]);
}

[[nodiscard]] inline std::uint64_t read_u64(const std::span<const std::byte> source) {
  std::uint64_t value = 0U;
  for (const auto byte : source.first<8U>()) {
    value = (value << 8U) | std::to_integer<std::uint64_t>(byte);
  }
  return value;
}

}  // namespace forgekv::protocol::wire
