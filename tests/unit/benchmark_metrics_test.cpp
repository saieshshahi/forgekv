#include "bench/system/metrics_sampler.h"

#include <chrono>
#include <limits>
#include <string>

#include <gtest/gtest.h>

namespace forgekv::benchmarking {
namespace {

TEST(BenchmarkMetricsTest, ParsesReorderedProcStatusWithOptionalFields) {
  ProcessSample sample;
  const auto error = parse_proc_status(
      "Name:\tforgekv\nnonvoluntary_ctxt_switches:\t3\n"
      "Threads:\t7\nVmRSS:\t2048 kB\nvoluntary_ctxt_switches:\t9\n",
      sample);
  EXPECT_FALSE(error.has_value());
  EXPECT_EQ(sample.rss_bytes, 2U * 1024U * 1024U);
  EXPECT_EQ(sample.threads, 7U);
  EXPECT_EQ(sample.voluntary_context_switches, 9U);
  EXPECT_EQ(sample.involuntary_context_switches, 3U);

  ProcessSample partial;
  EXPECT_FALSE(parse_proc_status("Threads:\t2\n", partial).has_value());
  EXPECT_EQ(partial.threads, 2U);
  EXPECT_FALSE(partial.rss_bytes.has_value());
}

TEST(BenchmarkMetricsTest, ProcParsersRejectDuplicatesOverflowAndLargeInput) {
  ProcessSample sample;
  EXPECT_TRUE(parse_proc_status("Threads:\t2\nThreads:\t3\n", sample)
                  .has_value());
  EXPECT_TRUE(parse_proc_status("VmRSS:\t-1 kB\n", sample).has_value());
  EXPECT_TRUE(parse_proc_status(
                  "VmRSS:\t18446744073709551615 kB\n", sample)
                  .has_value());
  EXPECT_TRUE(parse_proc_status(std::string(65U * 1024U, 'x'), sample)
                  .has_value());

  EXPECT_FALSE(parse_proc_io("write_bytes: 4096\nread_bytes: 1024\n", sample)
                   .has_value());
  EXPECT_EQ(sample.read_bytes, 1024U);
  EXPECT_EQ(sample.write_bytes, 4096U);
  EXPECT_TRUE(parse_proc_io("read_bytes: 1\nread_bytes: 2\n", sample)
                  .has_value());
}

TEST(BenchmarkMetricsTest, ParsesProcStatWithSpacesInProcessName) {
  ProcessSample sample;
  EXPECT_FALSE(parse_proc_stat(
                   "123 (forge kv server) S 1 2 3 4 5 6 7 8 9 10 100 25 0 0",
                   100U, sample)
                   .has_value());
  ASSERT_TRUE(sample.cpu_seconds.has_value());
  EXPECT_DOUBLE_EQ(*sample.cpu_seconds, 1.25);
  EXPECT_TRUE(parse_proc_stat("123 malformed", 100U, sample).has_value());
}

TEST(BenchmarkMetricsTest, ParsesRequestedPrometheusMetricsStrictly) {
  NodeSample sample;
  const auto error = parse_prometheus(
      "# HELP ignored comment\n"
      "forgekv_queue_depth 4\n"
      "forgekv_network_rx_bytes_total 1024\n"
      "forgekv_requests_total{op=\"put\"} 12\n"
      "forgekv_raft_replication_lag{peer=\"2\"} 3\n"
      "forgekv_raft_role{role=\"leader\"} 1\n"
      "forgekv_storage_sync_latency_seconds_bucket{le=\"0.001000\"} 8\n",
      sample);
  EXPECT_FALSE(error.has_value()) << error.value_or("");
  EXPECT_EQ(sample.role, "leader");
  EXPECT_EQ(sample.metric("forgekv_queue_depth"), 4.0);
  EXPECT_EQ(sample.metric("forgekv_network_rx_bytes_total"), 1024.0);
  EXPECT_EQ(sample.counter("forgekv_network_rx_bytes_total"), 1024U);
  EXPECT_EQ(sample.metric("forgekv_raft_replication_lag{peer=\"2\"}"), 3.0);
}

TEST(BenchmarkMetricsTest, RejectsBadPrometheusValuesLabelsAndDuplicates) {
  NodeSample sample;
  EXPECT_TRUE(parse_prometheus("forgekv_queue_depth NaN\n", sample)
                  .has_value());
  EXPECT_TRUE(parse_prometheus("forgekv_queue_depth +Inf\n", sample)
                  .has_value());
  EXPECT_TRUE(parse_prometheus(
                  "forgekv_queue_depth 18446744073709551616\n", sample)
                  .has_value());
  EXPECT_TRUE(parse_prometheus("forgekv_queue_depth -1\n", sample)
                  .has_value());
  EXPECT_TRUE(parse_prometheus(
                  "forgekv_queue_depth 1\nforgekv_queue_depth 2\n", sample)
                  .has_value());
  EXPECT_TRUE(parse_prometheus(
                  "forgekv_requests_total{op=\"unknown\"} 1\n", sample)
                  .has_value());
  EXPECT_TRUE(parse_prometheus(
                  "forgekv_raft_replication_lag{peer=\"0\"} 1\n", sample)
                  .has_value());
  EXPECT_TRUE(parse_prometheus(
                  "forgekv_request_latency_seconds_garbage{op=\"get\"} 1\n",
                  sample)
                  .has_value());
  EXPECT_TRUE(parse_prometheus(std::string(65U * 1024U, 'x'), sample)
                  .has_value());

  NodeSample partial;
  EXPECT_FALSE(parse_prometheus("forgekv_queue_depth 0\n", partial)
                   .has_value());
  EXPECT_FALSE(partial.role.has_value());
}

TEST(BenchmarkMetricsTest, CounterDeltaRequiresMonotonicFiniteSamples) {
  EXPECT_EQ(counter_delta(10.0, 14.5), 4.5);
  EXPECT_FALSE(counter_delta(14.5, 10.0).has_value());
  EXPECT_FALSE(counter_delta(std::nullopt, 10.0).has_value());
  EXPECT_FALSE(counter_delta(1.0, std::numeric_limits<double>::infinity())
                   .has_value());
}

}  // namespace
}  // namespace forgekv::benchmarking
