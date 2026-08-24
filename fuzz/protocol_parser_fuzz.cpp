#include "protocol/parser.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <span>

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, const std::size_t size) {
  const auto input = std::span<const std::byte>(reinterpret_cast<const std::byte*>(data), size);
  forgekv::protocol::Parser parser;

  std::size_t offset = 0U;
  while (offset < input.size()) {
    const auto requested = 1U + std::to_integer<std::size_t>(input[offset]) % 64U;
    const auto count = std::min(requested, input.size() - offset);
    const auto result = parser.consume(input.subspan(offset, count));
    if (!result.ok()) {
      return 0;
    }
    offset += count;
  }
  static_cast<void>(parser.finish());
  return 0;
}
