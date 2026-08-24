#include "protocol/serializer.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <string_view>
#include <vector>

namespace forgekv::protocol {
namespace {

std::vector<std::byte> bytes(const std::initializer_list<unsigned int> values) {
  std::vector<std::byte> result;
  result.reserve(values.size());
  for (const auto value : values) {
    result.push_back(static_cast<std::byte>(value));
  }
  return result;
}

std::vector<std::byte> text_bytes(const std::string_view text) {
  const auto* begin = reinterpret_cast<const std::byte*>(text.data());
  return {begin, begin + text.size()};
}

TEST(Serializer, ProducesExactBigEndianGoldenFrame) {
  const Frame frame{
      .message_namespace = Namespace::client,
      .message_type = MessageType::ping,
      .flags = 0U,
      .request_id = 0x0102030405060708ULL,
      .payload = text_bytes("abc"),
  };

  const auto result = serialize(frame);

  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result.bytes,
            bytes({0x46, 0x4B, 0x56, 0x31, 0x01, 0x01, 0x04, 0x00,
                   0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
                   0x00, 0x00, 0x00, 0x03, 0xB8, 0x4C, 0xB9, 0xFF,
                   0x61, 0x62, 0x63}));
}

TEST(Serializer, AcceptsZeroAndMaximumPayloads) {
  Frame empty{};
  empty.message_type = MessageType::ping;
  EXPECT_EQ(serialize(empty).bytes.size(), kHeaderSize);

  Frame maximum{};
  maximum.message_type = MessageType::put;
  maximum.payload.resize(kMaxPayloadSize, std::byte{0xA5});
  const auto result = serialize(maximum);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result.bytes.size(), kMaxFrameSize);
}

TEST(Serializer, RejectsPayloadBeyondHardLimitWithoutOutput) {
  Frame frame{};
  frame.message_type = MessageType::put;
  frame.payload.resize(kMaxPayloadSize + 1U);

  const auto result = serialize(frame);

  EXPECT_EQ(result.error, ProtocolError::payload_too_large);
  EXPECT_TRUE(result.bytes.empty());
}

TEST(Serializer, RejectsInvalidNamespaceTypeAndFlags) {
  Frame invalid_namespace{};
  invalid_namespace.message_namespace = static_cast<Namespace>(99U);
  EXPECT_EQ(serialize(invalid_namespace).error, ProtocolError::invalid_namespace);

  Frame invalid_type{};
  invalid_type.message_type = static_cast<MessageType>(0x7FU);
  EXPECT_EQ(serialize(invalid_type).error, ProtocolError::invalid_message_type);

  Frame wrong_namespace{};
  wrong_namespace.message_type = MessageType::raft_append_entries;
  EXPECT_EQ(serialize(wrong_namespace).error,
            ProtocolError::message_type_namespace_mismatch);

  Frame invalid_flags{};
  invalid_flags.flags = 1U;
  EXPECT_EQ(serialize(invalid_flags).error, ProtocolError::unsupported_flags);
}

}  // namespace
}  // namespace forgekv::protocol
