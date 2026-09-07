#include "chaos/client_worker.h"

#include "cluster/codecs.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>

#include <gtest/gtest.h>

namespace forgekv::chaos {
namespace {

ClientId client_id(const std::uint8_t suffix) {
  ClientId id{};
  id.back() = static_cast<std::byte>(suffix);
  return id;
}

TEST(ChaosClientStateTest, TimeoutRetriesSameIdBeforeNextOperation) {
  ClientState state(client_id(7U), "client-7");
  const auto first = state.begin_put("v1");
  EXPECT_FALSE(state.observe(first, AttemptResult::timeout()).completed);
  EXPECT_EQ(state.next_attempt()->request_id, first.request_id);
  EXPECT_TRUE(state.observe(first, AttemptResult::ok()).completed);
  ASSERT_TRUE(state.expected_value().has_value());
  EXPECT_EQ(*state.expected_value(), "v1");
  EXPECT_GT(state.begin_get().request_id, first.request_id);
}

TEST(ChaosClientStateTest, DeleteAndNotFoundUpdateOnlyDefinitiveState) {
  ClientState state(client_id(2U), "client-2");
  auto put = state.begin_put("present");
  ASSERT_TRUE(state.observe(put, AttemptResult::ok()).completed);
  auto remove = state.begin_delete();
  EXPECT_FALSE(state.observe(remove, AttemptResult::transport_error("reset"))
                   .completed);
  EXPECT_TRUE(state.expected_value().has_value());
  EXPECT_TRUE(state.observe(remove, AttemptResult::not_found()).completed);
  EXPECT_FALSE(state.expected_value().has_value());
}

TEST(ChaosClientStateTest, EncodesMutationClientAndKeyExactly) {
  ClientState state(client_id(9U), "owned-key");
  const auto request = state.begin_put("bytes");
  const auto frame = encode_request(request);
  const auto decoded = cluster::decode_client_mutation(frame);
  ASSERT_TRUE(decoded.ok()) << decoded.error;
  EXPECT_EQ(decoded.value->client_id, client_id(9U));
  EXPECT_EQ(decoded.value->request_id, request.request_id);
  EXPECT_EQ(decoded.value->key, "owned-key");
  EXPECT_EQ(std::string(reinterpret_cast<const char*>(decoded.value->value.data()),
                        decoded.value->value.size()),
            "bytes");
}

TEST(ChaosClientStateTest, ValidatesRedirectAndResponseIdentity) {
  const auto redirected = classify_response(
      protocol::Frame{.message_namespace = protocol::Namespace::client,
                      .message_type = protocol::MessageType::redirect,
                      .request_id = 3U,
                      .payload = redirect_payload("127.0.0.1:9911")},
      3U);
  ASSERT_EQ(redirected.kind, AttemptKind::redirect);
  ASSERT_TRUE(redirected.redirect.has_value());
  EXPECT_EQ(redirected.redirect->port, 9911U);

  const auto external = classify_response(
      protocol::Frame{.message_namespace = protocol::Namespace::client,
                      .message_type = protocol::MessageType::redirect,
                      .request_id = 3U,
                      .payload = redirect_payload("example.com:9911")},
      3U);
  EXPECT_EQ(external.kind, AttemptKind::protocol_error);
  EXPECT_EQ(classify_response(
                protocol::Frame{.message_namespace = protocol::Namespace::raft,
                                .message_type = protocol::MessageType::ok,
                                .request_id = 3U},
                3U)
                .kind,
            AttemptKind::protocol_error);
}

TEST(ChaosClientStateTest, RejectsStartingAnotherOutstandingRequest) {
  ClientState state(client_id(1U), "client-1");
  static_cast<void>(state.begin_put("v1"));
  EXPECT_THROW(static_cast<void>(state.begin_get()), std::logic_error);
}

}  // namespace
}  // namespace forgekv::chaos
