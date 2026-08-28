#include "cluster/state_machine.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace forgekv::cluster {
namespace {

ClientId client(const std::uint8_t value) {
  ClientId result{};
  std::ranges::fill(result, static_cast<std::byte>(value));
  return result;
}

ReplicatedCommand put_command(const ClientId& client_id,
                              const std::uint64_t request_id,
                              std::string key, const std::string& value) {
  const auto* begin = reinterpret_cast<const std::byte*>(value.data());
  return ReplicatedCommand{.operation = KvOperation::put,
                           .client_id = client_id,
                           .request_id = request_id,
                           .key = std::move(key),
                           .value = {begin, begin + value.size()}};
}

ReplicatedCommand delete_command(const ClientId& client_id,
                                 const std::uint64_t request_id,
                                 std::string key) {
  return ReplicatedCommand{.operation = KvOperation::delete_key,
                           .client_id = client_id,
                           .request_id = request_id,
                           .key = std::move(key),
                           .value = {}};
}

MutationApplyResult apply(ReplicatedStateMachine& state,
                          const ReplicatedCommand& command) {
  const auto canonical = encode_replicated_command(command);
  return state.apply(command, canonical);
}

TEST(ReplicatedStateMachineTest, DuplicateDeleteReturnsOriginalResultOnce) {
  ReplicatedStateMachine state;
  const auto id = client(0x11);
  EXPECT_EQ(apply(state, put_command(id, 1, "key", "value")).status,
            MutationApplyStatus::applied);

  const auto deletion = delete_command(id, 2, "key");
  const auto first = apply(state, deletion);
  ASSERT_EQ(first.status, MutationApplyStatus::applied);
  ASSERT_EQ(first.response.size(), 1U);
  EXPECT_EQ(first.response[0], std::byte{1});
  EXPECT_EQ(state.find("key"), nullptr);

  const auto duplicate = apply(state, deletion);
  EXPECT_EQ(duplicate.status, MutationApplyStatus::duplicate);
  EXPECT_EQ(duplicate.response, first.response);
  EXPECT_EQ(state.find("key"), nullptr);
}

TEST(ReplicatedStateMachineTest, RejectsChangedReuseAndStaleRequestIds) {
  ReplicatedStateMachine state;
  const auto id = client(0x22);
  EXPECT_EQ(apply(state, put_command(id, 7, "kept", "original")).status,
            MutationApplyStatus::applied);

  EXPECT_EQ(apply(state, put_command(id, 7, "kept", "changed")).status,
            MutationApplyStatus::request_id_reuse);
  EXPECT_EQ(apply(state, delete_command(id, 6, "kept")).status,
            MutationApplyStatus::stale_request);
  const auto* value = state.find("kept");
  ASSERT_NE(value, nullptr);
  EXPECT_EQ(std::string(reinterpret_cast<const char*>(value->data()),
                        value->size()),
            "original");
}

TEST(ReplicatedStateMachineTest, BoundsNewClientAdmissionWithoutEviction) {
  ReplicatedStateMachine state(1);
  EXPECT_EQ(apply(state, put_command(client(1), 1, "one", "accepted")).status,
            MutationApplyStatus::applied);
  EXPECT_EQ(apply(state, put_command(client(2), 1, "two", "rejected")).status,
            MutationApplyStatus::capacity_exceeded);
  EXPECT_EQ(state.client_count(), 1U);
  EXPECT_EQ(state.find("two"), nullptr);

  EXPECT_EQ(apply(state, put_command(client(1), 2, "three", "accepted"))
                .status,
            MutationApplyStatus::applied);
  EXPECT_NE(state.find("three"), nullptr);
}

TEST(ReplicatedStateMachineTest, BoundsRetainedCommandBytesBeforeMutation) {
  ReplicatedStateMachine state(2, 64);
  const auto id = client(1);
  EXPECT_EQ(apply(state, put_command(id, 1, "a", "b")).status,
            MutationApplyStatus::applied);

  EXPECT_EQ(apply(state, put_command(id, 2, "large", std::string(30, 'x')))
                .status,
            MutationApplyStatus::capacity_exceeded);
  EXPECT_EQ(state.find("large"), nullptr);

  EXPECT_EQ(apply(state, put_command(id, 2, "c", "d")).status,
            MutationApplyStatus::applied);
  EXPECT_NE(state.find("c"), nullptr);
}

TEST(ReplicatedStateMachineTest, SnapshotRestoreRetainsDuplicateResult) {
  ReplicatedStateMachine original;
  const auto id = client(0x33);
  EXPECT_EQ(apply(original, put_command(id, 1, "key", "value")).status,
            MutationApplyStatus::applied);
  const auto deletion = delete_command(id, 2, "key");
  const auto first = apply(original, deletion);
  ASSERT_EQ(first.response, (std::vector<std::byte>{std::byte{1}}));

  ReplicatedStateMachine restored;
  restored.restore(original.snapshot());
  const auto duplicate = apply(restored, deletion);
  EXPECT_EQ(duplicate.status, MutationApplyStatus::duplicate);
  EXPECT_EQ(duplicate.response, first.response);
  EXPECT_EQ(restored.find("key"), nullptr);
}

}  // namespace
}  // namespace forgekv::cluster
