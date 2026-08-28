#include "cluster/snapshot_codec.h"

#include "protocol/wire.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <vector>

namespace forgekv::cluster {
namespace {

std::vector<std::byte> value(const std::string& text) {
  const auto* begin = reinterpret_cast<const std::byte*>(text.data());
  return {begin, begin + text.size()};
}

ClientId client(const std::uint8_t value) {
  ClientId id{};
  std::ranges::fill(id, static_cast<std::byte>(value));
  return id;
}

DedupRecord record(const ClientId& id, const std::uint64_t request_id,
                   const std::string& key, const bool existed) {
  ReplicatedCommand command{.operation = KvOperation::delete_key,
                            .client_id = id,
                            .request_id = request_id,
                            .key = key,
                            .value = {}};
  return DedupRecord{.request_id = request_id,
                     .command = encode_replicated_command(command),
                     .response = {existed ? std::byte{1} : std::byte{0}}};
}

TEST(SnapshotCodecTest, DeterministicallyRoundTripsBinaryState) {
  const auto first_client = client(0x11);
  const auto second_client = client(0x22);
  const SnapshotState first{
      .values = {{"z", value("last")},
                 {"a", value(std::string("x\0y", 3))}},
      .clients = {{second_client, record(second_client, 9, "z", false)},
                  {first_client, record(first_client, 7, "a", true)}},
  };
  const SnapshotState second{
      .values = {{"a", value(std::string("x\0y", 3))},
                 {"z", value("last")}},
      .clients = {{first_client, record(first_client, 7, "a", true)},
                  {second_client, record(second_client, 9, "z", false)}},
  };
  const auto encoded = encode_snapshot_state(first);
  EXPECT_EQ(encoded, encode_snapshot_state(second));
  const auto decoded = decode_snapshot_state(encoded);
  ASSERT_TRUE(decoded.ok()) << decoded.error;
  EXPECT_EQ(*decoded.value, first);

  for (std::size_t size = 0; size < encoded.size(); ++size) {
    EXPECT_FALSE(decode_snapshot_state(std::vector<std::byte>(
                     encoded.begin(), encoded.begin() +
                                          static_cast<std::ptrdiff_t>(size)))
                     .ok())
        << size;
  }

  auto invalid_result = encoded;
  invalid_result.back() = std::byte{2};
  EXPECT_FALSE(decode_snapshot_state(invalid_result).ok());
}

TEST(SnapshotCodecTest, RejectsTruncationDuplicateKeysAndInvalidLengths) {
  auto encoded = encode_snapshot_state(
      SnapshotState{.values = {{"key", value("value")}}, .clients = {}});
  for (std::size_t size = 0; size < encoded.size(); ++size) {
    const std::vector<std::byte> prefix(encoded.begin(),
                                        encoded.begin() +
                                            static_cast<std::ptrdiff_t>(size));
    EXPECT_FALSE(decode_snapshot_state(prefix).ok()) << size;
  }

  auto duplicate = encode_snapshot_state(SnapshotState{
      .values = {{"a", value("1")}, {"b", value("2")}}, .clients = {}});
  ASSERT_GE(duplicate.size(), 43U);
  duplicate[42] = std::byte{'a'};
  EXPECT_FALSE(decode_snapshot_state(duplicate).ok());

  encoded[24] = std::byte{0x7F};
  EXPECT_FALSE(decode_snapshot_state(encoded).ok());
}

TEST(SnapshotCodecTest, RejectsInvalidSourceStateBeforeEncoding) {
  EXPECT_THROW(static_cast<void>(encode_snapshot_state(
                   SnapshotState{.values = {{"", value("value")}},
                                 .clients = {}})),
               std::invalid_argument);
  EXPECT_THROW(static_cast<void>(encode_snapshot_state(
                   SnapshotState{
                       .values = {{std::string(protocol::kMaxKeySize + 1U, 'k'),
                                   value("value")}},
                       .clients = {}})),
               std::invalid_argument);
  EXPECT_THROW(static_cast<void>(encode_snapshot_state(
                   SnapshotState{
                       .values = {{"key", std::vector<std::byte>(
                                               protocol::kMaxValueSize + 1U)}},
                       .clients = {}})),
               std::invalid_argument);

  const auto id = client(0x33);
  auto invalid_record = record(id, 1, "key", true);
  invalid_record.response = {std::byte{2}};
  EXPECT_THROW(static_cast<void>(encode_snapshot_state(
                   SnapshotState{.values = {},
                                 .clients = {{id, invalid_record}}})),
               std::invalid_argument);
}

TEST(SnapshotCodecTest, DecodesVersionOneAsStateWithoutDeduplication) {
  std::vector<std::byte> legacy(16U + 8U + 3U + 5U);
  protocol::wire::write_u32(std::span{legacy}.subspan(0, 4), 0x464B5653U);
  legacy[4] = std::byte{0};
  legacy[5] = std::byte{1};
  legacy[6] = std::byte{0};
  legacy[7] = std::byte{16};
  protocol::wire::write_u64(std::span{legacy}.subspan(8, 8), 1);
  protocol::wire::write_u32(std::span{legacy}.subspan(16, 4), 3);
  protocol::wire::write_u32(std::span{legacy}.subspan(20, 4), 5);
  std::ranges::copy(value("key"), legacy.begin() + 24);
  std::ranges::copy(value("value"), legacy.begin() + 27);

  const auto decoded = decode_snapshot_state(legacy);
  ASSERT_TRUE(decoded.ok()) << decoded.error;
  EXPECT_EQ(decoded.value->values,
            (KeyValueState{{"key", value("value")}}));
  EXPECT_TRUE(decoded.value->clients.empty());
}

}  // namespace
}  // namespace forgekv::cluster
