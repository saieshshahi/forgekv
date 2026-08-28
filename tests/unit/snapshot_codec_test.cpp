#include "cluster/snapshot_codec.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <string>
#include <vector>

namespace forgekv::cluster {
namespace {

std::vector<std::byte> value(const std::string& text) {
  const auto* begin = reinterpret_cast<const std::byte*>(text.data());
  return {begin, begin + text.size()};
}

TEST(SnapshotCodecTest, DeterministicallyRoundTripsBinaryState) {
  const SnapshotState first{{"z", value("last")},
                            {"a", value(std::string("x\0y", 3))}};
  const SnapshotState second{{"a", value(std::string("x\0y", 3))},
                             {"z", value("last")}};
  const auto encoded = encode_snapshot_state(first);
  EXPECT_EQ(encoded, encode_snapshot_state(second));
  const auto decoded = decode_snapshot_state(encoded);
  ASSERT_TRUE(decoded.ok()) << decoded.error;
  EXPECT_EQ(*decoded.value, first);
}

TEST(SnapshotCodecTest, RejectsTruncationDuplicateKeysAndInvalidLengths) {
  auto encoded = encode_snapshot_state(SnapshotState{{"key", value("value")}});
  for (std::size_t size = 0; size < encoded.size(); ++size) {
    const std::vector<std::byte> prefix(encoded.begin(),
                                        encoded.begin() +
                                            static_cast<std::ptrdiff_t>(size));
    EXPECT_FALSE(decode_snapshot_state(prefix).ok()) << size;
  }

  auto duplicate = encode_snapshot_state(
      SnapshotState{{"a", value("1")}, {"b", value("2")}});
  ASSERT_GE(duplicate.size(), 35U);
  duplicate[34] = std::byte{'a'};
  EXPECT_FALSE(decode_snapshot_state(duplicate).ok());

  encoded[16] = std::byte{0x7F};
  EXPECT_FALSE(decode_snapshot_state(encoded).ok());
}

TEST(SnapshotCodecTest, RejectsInvalidSourceStateBeforeEncoding) {
  EXPECT_THROW(static_cast<void>(encode_snapshot_state(
                   SnapshotState{{"", value("value")}})),
               std::invalid_argument);
  EXPECT_THROW(static_cast<void>(encode_snapshot_state(
                   SnapshotState{{std::string(protocol::kMaxKeySize + 1U, 'k'),
                                  value("value")}})),
               std::invalid_argument);
  EXPECT_THROW(static_cast<void>(encode_snapshot_state(
                   SnapshotState{{"key", std::vector<std::byte>(
                                              protocol::kMaxValueSize + 1U)}})),
               std::invalid_argument);
}

}  // namespace
}  // namespace forgekv::cluster
