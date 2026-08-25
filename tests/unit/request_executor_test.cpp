#include "server/request_executor.h"

#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <stdexcept>
#include <string>
#include <vector>

namespace forgekv::server {
namespace {

using namespace std::chrono_literals;

protocol::Frame request(const std::uint64_t id, const std::size_t payload_size = 0U) {
  protocol::Frame frame{
      .message_namespace = protocol::Namespace::client,
      .message_type = protocol::MessageType::ping,
      .flags = 0U,
      .request_id = id,
      .payload = {},
  };
  frame.payload.resize(payload_size, std::byte{0x2A});
  return frame;
}

class CompletionCollector {
 public:
  void add(Completion completion) {
    {
      const std::lock_guard lock(mutex_);
      completions_.push_back(std::move(completion));
    }
    condition_.notify_all();
  }

  std::vector<Completion> wait_for(const std::size_t count) {
    std::unique_lock lock(mutex_);
    const auto ready = condition_.wait_for(lock, 2s, [&] { return completions_.size() >= count; });
    EXPECT_TRUE(ready);
    return completions_;
  }

 private:
  std::mutex mutex_;
  std::condition_variable condition_;
  std::vector<Completion> completions_;
};

TEST(RequestExecutor, ExecutesHandlerAndPreservesConnectionToken) {
  CompletionCollector collector;
  RequestExecutor executor(
      1U, 4U, 4096U,
      [](const protocol::Frame& input) {
        auto output = input;
        output.message_type = protocol::MessageType::ok;
        return output;
      },
      [&collector](Completion completion) { collector.add(std::move(completion)); });

  EXPECT_TRUE(executor.try_submit(Request{.token = {.id = 5U, .generation = 7U},
                                          .frame = request(11U),
                                          .wire_bytes = protocol::kHeaderSize}));
  const auto completions = collector.wait_for(1U);

  ASSERT_EQ(completions.size(), 1U);
  EXPECT_EQ(completions.front().token, (net::ConnectionToken{5U, 7U}));
  EXPECT_EQ(completions.front().frame.message_type, protocol::MessageType::ok);
  EXPECT_EQ(completions.front().frame.request_id, 11U);
}

TEST(RequestExecutor, RejectsWorkBeyondItemAndByteCaps) {
  std::mutex gate_mutex;
  std::condition_variable gate_condition;
  bool release = false;
  CompletionCollector collector;
  RequestExecutor executor(
      1U, 1U, 64U,
      [&](const protocol::Frame& input) {
        std::unique_lock lock(gate_mutex);
        gate_condition.wait(lock, [&] { return release; });
        return input;
      },
      [&collector](Completion completion) { collector.add(std::move(completion)); });

  EXPECT_TRUE(executor.try_submit(Request{.token = {},
                                          .frame = request(1U),
                                          .wire_bytes = 40U}));
  EXPECT_FALSE(executor.try_submit(Request{.token = {},
                                           .frame = request(2U),
                                           .wire_bytes = 1U}));

  {
    const std::lock_guard lock(gate_mutex);
    release = true;
  }
  gate_condition.notify_all();
  executor.stop();
  EXPECT_EQ(collector.wait_for(1U).size(), 1U);
}

TEST(RequestExecutor, ConvertsHandlerExceptionToErrorResponse) {
  CompletionCollector collector;
  RequestExecutor executor(
      1U, 2U, 1024U,
      [](const protocol::Frame&) -> protocol::Frame { throw std::runtime_error("handler failed"); },
      [&collector](Completion completion) { collector.add(std::move(completion)); });

  EXPECT_TRUE(executor.try_submit(Request{.token = {},
                                          .frame = request(99U),
                                          .wire_bytes = protocol::kHeaderSize}));
  const auto completions = collector.wait_for(1U);

  ASSERT_EQ(completions.size(), 1U);
  EXPECT_EQ(completions.front().frame.message_type, protocol::MessageType::error);
  EXPECT_EQ(completions.front().frame.request_id, 99U);
  EXPECT_FALSE(completions.front().frame.payload.empty());
}

TEST(RequestExecutor, StopDrainsAcceptedWorkAndIsIdempotent) {
  CompletionCollector collector;
  RequestExecutor executor(
      1U, 2U, 1024U, [](const protocol::Frame& input) { return input; },
      [&collector](Completion completion) { collector.add(std::move(completion)); });

  EXPECT_TRUE(executor.try_submit(Request{.token = {},
                                          .frame = request(1U),
                                          .wire_bytes = protocol::kHeaderSize}));
  EXPECT_TRUE(executor.try_submit(Request{.token = {},
                                          .frame = request(2U),
                                          .wire_bytes = protocol::kHeaderSize}));
  executor.stop();
  executor.stop();

  EXPECT_EQ(collector.wait_for(2U).size(), 2U);
  EXPECT_FALSE(executor.try_submit(Request{.token = {},
                                           .frame = request(3U),
                                           .wire_bytes = protocol::kHeaderSize}));
}

}  // namespace
}  // namespace forgekv::server
