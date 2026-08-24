#include "protocol/checksum.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <span>
#include <string_view>

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

}  // namespace
}  // namespace forgekv::protocol
