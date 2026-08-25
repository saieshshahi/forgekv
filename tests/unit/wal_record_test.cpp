#include "storage/wal_record.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace forgekv::storage {
namespace {

TEST(WalRecordTest, PutHasStableGoldenEncoding) {
  const WalRecord record{0x0102030405060708ULL, WalOperation::put, "k", "v"};
  const auto encoded = encode_record(record);

  const std::vector<std::byte> expected{
      std::byte{0x46}, std::byte{0x57}, std::byte{0x41}, std::byte{0x4c},
      std::byte{0x00}, std::byte{0x01}, std::byte{0x00}, std::byte{0x24},
      std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x26},
      std::byte{0x01}, std::byte{0x02}, std::byte{0x03}, std::byte{0x04},
      std::byte{0x05}, std::byte{0x06}, std::byte{0x07}, std::byte{0x08},
      std::byte{0x01}, std::byte{0x00}, std::byte{0x00}, std::byte{0x00},
      std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x01},
      std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x01},
      std::byte{0xd3}, std::byte{0xbd}, std::byte{0xc7}, std::byte{0x02},
      std::byte{0x6b}, std::byte{0x76},
  };
  EXPECT_EQ(encoded, expected);
  const auto decoded = decode_record(encoded);
  ASSERT_EQ(decoded.status, DecodeStatus::complete);
  EXPECT_EQ(decoded.record, record);
  EXPECT_EQ(decoded.consumed, expected.size());
}

TEST(WalRecordTest, DeleteHasNoValue) {
  const WalRecord record{9U, WalOperation::delete_key, "key", {}};
  const auto decoded = decode_record(encode_record(record));
  ASSERT_EQ(decoded.status, DecodeStatus::complete);
  EXPECT_EQ(decoded.record, record);
}

TEST(WalRecordTest, AcceptsBinaryKeysAndValues) {
  const WalRecord record{1U, WalOperation::put,
                         std::string{"a\0b", 3}, std::string{"\0\xff", 2}};
  const auto decoded = decode_record(encode_record(record));
  ASSERT_EQ(decoded.status, DecodeStatus::complete);
  EXPECT_EQ(decoded.record, record);
}

TEST(WalRecordTest, RejectsInvalidRecordsBeforeEncoding) {
  EXPECT_THROW(static_cast<void>(
                   encode_record({0U, WalOperation::put, "k", "v"})),
               std::invalid_argument);
  EXPECT_THROW(static_cast<void>(
                   encode_record({1U, WalOperation::put, "", "v"})),
               std::invalid_argument);
  EXPECT_THROW(static_cast<void>(encode_record(
                   {1U, WalOperation::delete_key, "k", "v"})),
               std::invalid_argument);
  EXPECT_THROW(static_cast<void>(encode_record(
                   {1U, WalOperation::put,
                    std::string(kMaxKeySize + 1U, 'k'), {}})),
               std::invalid_argument);
  EXPECT_THROW(static_cast<void>(encode_record(
                   {1U, WalOperation::put, "k",
                    std::string(kMaxValueSize + 1U, 'v')})),
               std::invalid_argument);
}

TEST(WalRecordTest, ReportsEveryPrefixAsIncomplete) {
  const auto encoded = encode_record({1U, WalOperation::put, "key", "value"});
  for (std::size_t length = 0U; length < encoded.size(); ++length) {
    const auto result = decode_record(std::span{encoded}.first(length));
    EXPECT_EQ(result.status, DecodeStatus::incomplete) << length;
    EXPECT_EQ(result.consumed, 0U) << length;
  }
}

TEST(WalRecordTest, RejectsStructuralAndChecksumCorruption) {
  const auto valid = encode_record({1U, WalOperation::put, "key", "value"});
  for (const std::size_t offset : std::array<std::size_t, 7U>{0U, 5U, 7U, 20U,
                                                               21U, 27U, 35U}) {
    auto corrupted = valid;
    corrupted[offset] ^= std::byte{0x01};
    EXPECT_EQ(decode_record(corrupted).status, DecodeStatus::corrupt) << offset;
  }
}

TEST(WalRecordTest, ConsumesOnlyOneRecord) {
  const auto first = encode_record({1U, WalOperation::put, "a", "b"});
  const auto second = encode_record({2U, WalOperation::delete_key, "a", {}});
  auto both = first;
  both.insert(both.end(), second.begin(), second.end());
  const auto decoded = decode_record(both);
  EXPECT_EQ(decoded.status, DecodeStatus::complete);
  EXPECT_EQ(decoded.consumed, first.size());
}

}  // namespace
}  // namespace forgekv::storage
