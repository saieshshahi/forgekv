#include "net/reactor_config.h"

#include <gtest/gtest.h>

namespace forgekv::net {
namespace {

TEST(ReactorConfig, DefaultsAreInternallyConsistent) {
  const ReactorConfig config;

  EXPECT_FALSE(config.validation_error().has_value());
  EXPECT_GE(config.max_buffered_input_per_connection, config.max_request_size);
  EXPECT_GE(config.global_outstanding_work, config.max_requests_in_flight_per_connection);
}

TEST(ReactorConfig, RejectsZeroAndContradictoryLimits) {
  ReactorConfig zero_workers;
  zero_workers.worker_threads = 0U;
  EXPECT_TRUE(zero_workers.validation_error().has_value());

  ReactorConfig input_too_small;
  input_too_small.max_buffered_input_per_connection =
      input_too_small.max_request_size - 1U;
  EXPECT_TRUE(input_too_small.validation_error().has_value());

  ReactorConfig global_too_small;
  global_too_small.global_outstanding_work =
      global_too_small.max_requests_in_flight_per_connection - 1U;
  EXPECT_TRUE(global_too_small.validation_error().has_value());

  ReactorConfig oversized_request;
  oversized_request.max_request_size = protocol::kMaxFrameSize + 1U;
  EXPECT_TRUE(oversized_request.validation_error().has_value());
}

}  // namespace
}  // namespace forgekv::net
