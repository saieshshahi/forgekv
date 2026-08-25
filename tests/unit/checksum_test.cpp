#include "protocol/checksum.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>
#include <vector>

namespace forgekv::protocol {
namespace {

std::span<const std::byte> as_bytes(const std::string_view text) {
  return {reinterpret_cast<const std::byte*>(text.data()), text.size()};
}

TEST(Checksum, EmptyInputUsesStandardCrc32Value) {
  EXPECT_EQ(crc32({}), 0U);
}

TEST(Checksum, MatchesStandardNineDigitVector) {
  EXPECT_EQ(crc32(as_bytes("123456789")), 0xCBF43926U);
}

TEST(Checksum, MatchesAllByteValuesVector) {
  std::vector<std::byte> bytes;
  bytes.reserve(256);
  for (std::uint16_t value = 0; value < 256; ++value) {
    bytes.push_back(static_cast<std::byte>(value));
  }

  EXPECT_EQ(crc32(bytes), 0x29058C73U);
}

TEST(Checksum, IncrementalUpdatesMatchSingleUpdateAcrossEveryBoundary) {
  constexpr std::string_view input =
      "slicing by eight must preserve incremental CRC32 semantics";
  const auto bytes = as_bytes(input);
  const auto expected = crc32(bytes);

  for (std::size_t split = 0; split <= bytes.size(); ++split) {
    Crc32 checksum;
    checksum.update(bytes.first(split));
    checksum.update(bytes.subspan(split));
    EXPECT_EQ(checksum.value(), expected) << "split=" << split;
  }
}

}  // namespace
}  // namespace forgekv::protocol
