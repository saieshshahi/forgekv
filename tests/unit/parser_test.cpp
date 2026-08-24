#include "protocol/parser.h"

#include "protocol/serializer.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <random>
#include <span>
#include <string_view>
#include <vector>

namespace forgekv::protocol {
namespace {

std::vector<std::byte> payload(const std::string_view text) {
  const auto* begin = reinterpret_cast<const std::byte*>(text.data());
  return {begin, begin + text.size()};
}

Frame sample_frame(const std::uint64_t request_id, const std::string_view text) {
  return Frame{
      .message_namespace = Namespace::client,
      .message_type = MessageType::ping,
      .flags = 0U,
      .request_id = request_id,
      .payload = payload(text),
  };
}

std::vector<Frame> feed_in_chunks(Parser& parser, const std::vector<std::byte>& wire,
                                  const std::vector<std::size_t>& chunk_sizes) {
  std::vector<Frame> frames;
  std::size_t offset = 0U;
  std::size_t chunk_index = 0U;
  while (offset < wire.size()) {
    const auto requested = chunk_sizes[chunk_index % chunk_sizes.size()];
    const auto count = std::min(requested, wire.size() - offset);
    const auto batch = parser.consume(std::span<const std::byte>(wire).subspan(offset, count));
    EXPECT_TRUE(batch.ok());
    frames.insert(frames.end(), batch.frames.begin(), batch.frames.end());
    offset += count;
    ++chunk_index;
  }
  return frames;
}

TEST(Parser, EmitsFrameWhenFedOneByteAtATime) {
  const auto expected = sample_frame(41U, "fragmented");
  const auto wire = serialize(expected).bytes;
  Parser parser;

  const auto frames = feed_in_chunks(parser, wire, {1U});

  EXPECT_EQ(frames, std::vector<Frame>{expected});
  EXPECT_EQ(parser.buffered_bytes(), 0U);
  EXPECT_EQ(parser.finish(), ProtocolError::none);
}

TEST(Parser, HandlesDeterministicRandomFragmentation) {
  const auto expected = sample_frame(42U, std::string(4096U, 'v'));
  const auto wire = serialize(expected).bytes;
  std::mt19937 generator(0xF0473U);
  std::uniform_int_distribution<std::size_t> distribution(1U, 97U);
  std::vector<std::size_t> chunks(128U);
  std::generate(chunks.begin(), chunks.end(), [&] { return distribution(generator); });
  Parser parser;

  const auto frames = feed_in_chunks(parser, wire, chunks);

  EXPECT_EQ(frames, std::vector<Frame>{expected});
}

TEST(Parser, EmitsMultipleConcatenatedFramesFromOneChunk) {
  const auto first = sample_frame(1U, "first");
  const auto second = sample_frame(2U, "second");
  auto wire = serialize(first).bytes;
  const auto second_wire = serialize(second).bytes;
  wire.insert(wire.end(), second_wire.begin(), second_wire.end());
  Parser parser;

  const auto batch = parser.consume(wire);

  ASSERT_TRUE(batch.ok());
  EXPECT_EQ(batch.frames, (std::vector<Frame>{first, second}));
}

TEST(Parser, AcceptsZeroAndMaximumPayloads) {
  Frame empty = sample_frame(1U, "");
  Frame maximum = sample_frame(2U, "");
  maximum.message_type = MessageType::put;
  maximum.payload.resize(kMaxPayloadSize, std::byte{0x5A});
  auto wire = serialize(empty).bytes;
  const auto maximum_wire = serialize(maximum).bytes;
  wire.insert(wire.end(), maximum_wire.begin(), maximum_wire.end());
  Parser parser;

  const auto batch = parser.consume(wire);

  ASSERT_TRUE(batch.ok());
  EXPECT_EQ(batch.frames, (std::vector<Frame>{empty, maximum}));
}

TEST(Parser, ReportsTruncatedHeaderAndPayloadOnlyAtEndOfStream) {
  const auto wire = serialize(sample_frame(7U, "payload")).bytes;

  Parser header_parser;
  EXPECT_TRUE(header_parser.consume(std::span<const std::byte>(wire).first(3U)).ok());
  EXPECT_EQ(header_parser.finish(), ProtocolError::truncated_frame);

  Parser payload_parser;
  EXPECT_TRUE(payload_parser.consume(std::span<const std::byte>(wire).first(kHeaderSize + 2U)).ok());
  EXPECT_EQ(payload_parser.finish(), ProtocolError::truncated_frame);
}

TEST(Parser, RejectsMalformedHeaderFieldsBeforePayloadAllocation) {
  const auto valid = serialize(sample_frame(9U, "x")).bytes;

  auto bad_magic = valid;
  bad_magic[0] = std::byte{0};
  Parser magic_parser;
  EXPECT_EQ(magic_parser.consume(bad_magic).error, ProtocolError::invalid_magic);

  auto bad_version = valid;
  bad_version[4] = std::byte{99};
  Parser version_parser;
  EXPECT_EQ(version_parser.consume(bad_version).error, ProtocolError::unsupported_version);

  auto bad_namespace = valid;
  bad_namespace[5] = std::byte{99};
  Parser namespace_parser;
  EXPECT_EQ(namespace_parser.consume(bad_namespace).error, ProtocolError::invalid_namespace);

  auto bad_type = valid;
  bad_type[6] = std::byte{0x7F};
  Parser type_parser;
  EXPECT_EQ(type_parser.consume(bad_type).error, ProtocolError::invalid_message_type);

  auto bad_flags = valid;
  bad_flags[7] = std::byte{1};
  Parser flags_parser;
  EXPECT_EQ(flags_parser.consume(bad_flags).error, ProtocolError::unsupported_flags);

  auto oversized = valid;
  oversized[16] = std::byte{0x7F};
  oversized[17] = std::byte{0xFF};
  oversized[18] = std::byte{0xFF};
  oversized[19] = std::byte{0xFF};
  Parser length_parser;
  const auto result = length_parser.consume(oversized);
  EXPECT_EQ(result.error, ProtocolError::payload_too_large);
  EXPECT_EQ(length_parser.buffered_bytes(), 0U);
}

TEST(Parser, RejectsNamespaceTypeMismatch) {
  auto wire = serialize(Frame{.message_namespace = Namespace::raft,
                              .message_type = MessageType::raft_append_entries,
                              .request_id = 3U,
                              .payload = {}})
                  .bytes;
  wire[5] = static_cast<std::byte>(Namespace::client);
  Parser parser;

  EXPECT_EQ(parser.consume(wire).error,
            ProtocolError::message_type_namespace_mismatch);
}

TEST(Parser, RejectsCorruptedChecksumAndRemainsTerminal) {
  auto wire = serialize(sample_frame(11U, "checksum")).bytes;
  wire.back() ^= std::byte{0x01};
  Parser parser;

  EXPECT_EQ(parser.consume(wire).error, ProtocolError::checksum_mismatch);
  EXPECT_EQ(parser.consume(wire).error, ProtocolError::parser_failed);
  EXPECT_TRUE(parser.failed());
}

}  // namespace
}  // namespace forgekv::protocol
