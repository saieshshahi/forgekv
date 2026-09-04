#include "net/metrics.h"

#include <gtest/gtest.h>

namespace forgekv::net {
namespace {

TEST(NetworkMetrics, CloseWithoutOpenCannotUnderflowActiveConnections) {
  Metrics metrics;

  metrics.connection_closed();

  const auto snapshot = metrics.snapshot();
  EXPECT_EQ(snapshot.active_connections, 0U);
  EXPECT_EQ(snapshot.closed_connections_total, 0U);
}

TEST(NetworkMetrics, CountersAndGaugesProduceExactSnapshot) {
  Metrics metrics;

  metrics.connection_opened();
  metrics.add_bytes_read(120U);
  metrics.add_bytes_written(80U);
  metrics.request_started();
  metrics.add_read_buffer_bytes(24U);
  metrics.add_write_buffer_bytes(48U);
  metrics.backpressure_started();
  metrics.request_rejected();

  const auto busy = metrics.snapshot();
  EXPECT_EQ(busy.active_connections, 1U);
  EXPECT_EQ(busy.accepted_connections_total, 1U);
  EXPECT_EQ(busy.closed_connections_total, 0U);
  EXPECT_EQ(busy.bytes_read_total, 120U);
  EXPECT_EQ(busy.bytes_written_total, 80U);
  EXPECT_EQ(busy.requests_in_flight, 1U);
  EXPECT_EQ(busy.read_buffer_bytes, 24U);
  EXPECT_EQ(busy.write_buffer_bytes, 48U);
  EXPECT_EQ(busy.connections_backpressured, 1U);
  EXPECT_EQ(busy.backpressure_events_total, 1U);
  EXPECT_EQ(busy.rejected_requests_total, 1U);

  metrics.request_finished();
  metrics.remove_read_buffer_bytes(24U);
  metrics.remove_write_buffer_bytes(48U);
  metrics.backpressure_ended();
  metrics.connection_closed();

  const auto idle = metrics.snapshot();
  EXPECT_EQ(idle.active_connections, 0U);
  EXPECT_EQ(idle.accepted_connections_total, 1U);
  EXPECT_EQ(idle.closed_connections_total, 1U);
  EXPECT_EQ(idle.requests_in_flight, 0U);
  EXPECT_EQ(idle.read_buffer_bytes, 0U);
  EXPECT_EQ(idle.write_buffer_bytes, 0U);
  EXPECT_EQ(idle.connections_backpressured, 0U);
  EXPECT_EQ(idle.backpressure_events_total, 1U);
  EXPECT_EQ(idle.rejected_requests_total, 1U);
}

}  // namespace
}  // namespace forgekv::net
