#include "cluster/codecs.h"
#include "cluster/node.h"

#include "protocol/wire.h"
#include "raft/raft_storage.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <limits>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

namespace forgekv::cluster {
namespace {

TEST(ClusterEndpoints, ParsesIpv4AndBracketedIpv6PeerSpecifications) {
  EXPECT_EQ(parse_peer_address("7=127.0.0.1:7207:7107"),
            (PeerAddress{.node_id = 7,
                         .host = "127.0.0.1",
                         .peer_port = 7207,
                         .client_port = 7107}));
  EXPECT_EQ(parse_peer_address("9=[2001:db8::9]:7209:7109"),
            (PeerAddress{.node_id = 9,
                         .host = "2001:db8::9",
                         .peer_port = 7209,
                         .client_port = 7109}));
  EXPECT_THROW(static_cast<void>(
                   parse_peer_address("9=2001:db8::9:7209:7109")),
               std::invalid_argument);
}

TEST(ClusterEndpoints, FormatsUnambiguousRedirectEndpoints) {
  EXPECT_EQ(format_endpoint("127.0.0.1", 7101), "127.0.0.1:7101");
  EXPECT_EQ(format_endpoint("2001:db8::1", 7101), "[2001:db8::1]:7101");
}

raft::LogEntry entry(const raft::LogIndex index, const raft::Term term,
                     const std::string& text) {
  const auto* begin = reinterpret_cast<const std::byte*>(text.data());
  return raft::LogEntry{.index = index,
                        .term = term,
                        .kind = raft::EntryKind::command,
                        .command = {begin, begin + text.size()}};
}

TEST(ClusterCodecs, EveryRaftMessageRoundTripsWithIdentity) {
  const std::vector<raft::Message> messages{
      raft::RequestVote{.term = 4,
                        .candidate_id = 2,
                        .last_log_index = 9,
                        .last_log_term = 3},
      raft::RequestVoteResponse{.term = 4, .vote_granted = true},
      raft::AppendEntries{.term = 5,
                          .leader_id = 1,
                          .previous_log_index = 1,
                          .previous_log_term = 4,
                          .entries = {entry(2, 5, "alpha"),
                                      entry(3, 5, "beta")},
                          .leader_commit = 2,
                          .rpc_id = 77},
      raft::AppendEntriesResponse{.term = 5,
                                  .success = false,
                                  .match_index = 1,
                                  .reject_hint = 2,
                                  .rpc_id = 77},
      raft::InstallSnapshot{.term = 6,
                            .leader_id = 1,
                            .last_included_index = 41,
                            .last_included_term = 5,
                            .total_size = 3,
                            .offset = 1,
                            .data = {std::byte{0x02}, std::byte{0x03}},
                            .done = true,
                            .rpc_id = 78},
      raft::InstallSnapshotResponse{.term = 6,
                                    .success = true,
                                    .last_included_index = 41,
                                    .next_offset = 3,
                                    .rpc_id = 78},
  };
  for (const auto& message : messages) {
    const PeerEnvelope expected{.cluster_id = 91,
                                .from = 1,
                                .to = 2,
                                .message = message};
    const auto decoded = decode_peer_frame(encode_peer_frame(expected, 123));
    ASSERT_TRUE(decoded.ok()) << decoded.error;
    EXPECT_EQ(*decoded.value, expected);
  }
}

TEST(ClusterCodecs, RejectsWrongNamespaceReservedBytesAndTruncation) {
  const PeerEnvelope envelope{
      .cluster_id = 91,
      .from = 1,
      .to = 2,
      .message = raft::AppendEntries{.term = 1,
                                     .leader_id = 1,
                                     .previous_log_index = 0,
                                     .previous_log_term = 0,
                                     .entries = {entry(1, 1, "value")},
                                     .leader_commit = 0,
                                     .rpc_id = 1},
  };
  auto frame = encode_peer_frame(envelope, 1);
  frame.message_namespace = protocol::Namespace::client;
  EXPECT_FALSE(decode_peer_frame(frame).ok());
  frame = encode_peer_frame(envelope, 1);
  frame.payload[25] = std::byte{1};
  EXPECT_FALSE(decode_peer_frame(frame).ok());
  frame = encode_peer_frame(envelope, 1);
  frame.payload.pop_back();
  EXPECT_FALSE(decode_peer_frame(frame).ok());
}

TEST(ClusterCodecs, RejectsImpossibleEntryCountBeforeAllocating) {
  const PeerEnvelope envelope{
      .cluster_id = 91,
      .from = 1,
      .to = 2,
      .message = raft::AppendEntries{.term = 1,
                                     .leader_id = 1,
                                     .previous_log_index = 0,
                                     .previous_log_term = 0,
                                     .entries = {},
                                     .leader_commit = 0,
                                     .rpc_id = 1},
  };
  auto frame = encode_peer_frame(envelope, 1);
  protocol::wire::write_u32(std::span{frame.payload}.subspan(80, 4),
                            std::numeric_limits<std::uint32_t>::max());

  EXPECT_NO_THROW({ EXPECT_FALSE(decode_peer_frame(frame).ok()); });
}

TEST(ClusterCodecs, RejectsStorageInvalidPeerEntries) {
  PeerEnvelope envelope{
      .cluster_id = 91,
      .from = 1,
      .to = 2,
      .message = raft::AppendEntries{.term = 1,
                                     .leader_id = 1,
                                     .previous_log_index = 0,
                                     .previous_log_term = 0,
                                     .entries = {entry(1, 1, "x")},
                                     .leader_commit = 0,
                                     .rpc_id = 1},
  };
  auto frame = encode_peer_frame(envelope, 1);
  frame.payload[104] = static_cast<std::byte>(raft::EntryKind::no_op);
  EXPECT_FALSE(decode_peer_frame(frame).ok());

  frame = encode_peer_frame(envelope, 1);
  protocol::wire::write_u64(std::span{frame.payload}.subspan(96, 8), 0);
  EXPECT_FALSE(decode_peer_frame(frame).ok());

  auto& append = std::get<raft::AppendEntries>(envelope.message);
  append.entries.assign(raft::kMaxRaftLogEntriesPerRecord + 1U,
                        raft::LogEntry{.index = 1,
                                       .term = 1,
                                       .kind = raft::EntryKind::no_op,
                                       .command = {}});
  EXPECT_THROW(static_cast<void>(encode_peer_frame(envelope, 1)),
               std::invalid_argument);
}

TEST(ClusterCodecs, ClientMutationBecomesDeterministicReplicatedCommand) {
  std::vector<std::byte> payload(24U + 3U + 5U);
  for (std::size_t index = 0; index < 16U; ++index) {
    payload[index] = static_cast<std::byte>(index);
  }
  protocol::wire::write_u32(std::span{payload}.subspan(16, 4), 3U);
  protocol::wire::write_u32(std::span{payload}.subspan(20, 4), 5U);
  const std::string key = "key";
  const std::string value = "value";
  std::ranges::transform(key, payload.begin() + 24, [](const char character) {
    return static_cast<std::byte>(static_cast<unsigned char>(character));
  });
  std::ranges::transform(value, payload.begin() + 27,
                         [](const char character) {
                           return static_cast<std::byte>(
                               static_cast<unsigned char>(character));
                         });
  const protocol::Frame frame{
      .message_namespace = protocol::Namespace::client,
      .message_type = protocol::MessageType::put,
      .flags = 0,
      .request_id = 88,
      .payload = payload,
  };

  const auto decoded_client = decode_client_mutation(frame);
  ASSERT_TRUE(decoded_client.ok()) << decoded_client.error;
  const auto encoded = encode_replicated_command(*decoded_client.value);
  const auto decoded_log = decode_replicated_command(encoded);
  ASSERT_TRUE(decoded_log.ok()) << decoded_log.error;
  EXPECT_EQ(*decoded_log.value, *decoded_client.value);
  EXPECT_EQ(decoded_log.value->key, key);
  EXPECT_EQ(decoded_log.value->value,
            (std::vector<std::byte>{std::byte{0x76}, std::byte{0x61},
                                    std::byte{0x6C}, std::byte{0x75},
                                    std::byte{0x65}}));
}

TEST(ClusterCodecs, RejectsMalformedClientAndReplicatedLengths) {
  protocol::Frame get{.message_namespace = protocol::Namespace::client,
                      .message_type = protocol::MessageType::get,
                      .request_id = 1,
                      .payload = std::vector<std::byte>(5)};
  protocol::wire::write_u32(std::span{get.payload}.first<4>(), 9U);
  EXPECT_FALSE(decode_client_get(get).ok());

  ReplicatedCommand command{.operation = KvOperation::put,
                            .request_id = 1,
                            .key = "key",
                            .value = {std::byte{1}}};
  auto encoded = encode_replicated_command(command);
  protocol::wire::write_u32(std::span{encoded}.subspan(36, 4), 99U);
  EXPECT_FALSE(decode_replicated_command(encoded).ok());
}

}  // namespace
}  // namespace forgekv::cluster
