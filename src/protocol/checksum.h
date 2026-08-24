#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

namespace forgekv::protocol {

class Crc32 final {
 public:
  void update(std::span<const std::byte> bytes) noexcept;
  [[nodiscard]] std::uint32_t value() const noexcept;

 private:
  std::uint32_t remainder_{0xFFFFFFFFU};
};

[[nodiscard]] std::uint32_t crc32(std::span<const std::byte> bytes) noexcept;

}  // namespace forgekv::protocol
