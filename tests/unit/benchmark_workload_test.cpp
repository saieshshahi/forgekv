#include "bench/system/latency_histogram.h"
#include "bench/system/workload.h"

#include <chrono>
#include <cstddef>
#include <set>
#include <string>

#include <gtest/gtest.h>

namespace forgekv::benchmarking {
namespace {

using namespace std::chrono_literals;

TEST(BenchmarkScenarioTest, RequiredClusterMatrixIsExactAndStable) {
  const auto matrix = required_cluster_matrix();
  ASSERT_EQ(matrix.size(), 3U * 5U * 4U * 5U);

  std::set<std::string> ids;
  for (const auto& scenario : matrix) {
    EXPECT_FALSE(validate(scenario).has_value());
    EXPECT_EQ(scenario.scope, BenchmarkScope::cluster);
    EXPECT_EQ(scenario.durability, DurabilityLabel::quorum_sync);
    EXPECT_TRUE(ids.insert(scenario_id(scenario)).second);
  }
  EXPECT_EQ(scenario_id(matrix.front()), scenario_id(matrix.front()));
}

TEST(BenchmarkScenarioTest, RejectsInvalidScopeAndPublishableTiming) {
  Scenario scenario = required_cluster_matrix().front();
  scenario.durability = DurabilityLabel::async;
  EXPECT_TRUE(validate(scenario).has_value());

  scenario.durability = DurabilityLabel::quorum_sync;
  scenario.measurement = 2s;
  EXPECT_TRUE(validate(scenario).has_value());

  scenario.publishable = false;
  EXPECT_FALSE(validate(scenario).has_value());

  scenario.scope = BenchmarkScope::storage;
  scenario.durability = DurabilityLabel::sync;
  scenario.nodes = 3U;
  EXPECT_TRUE(validate(scenario).has_value());
}

TEST(BenchmarkScenarioTest, RejectsUnknownEnumValues) {
  Scenario scenario = required_cluster_matrix().front();
  scenario.scope = static_cast<BenchmarkScope>(255U);
  scenario.nodes = 1U;
  scenario.durability = DurabilityLabel::sync;
  EXPECT_TRUE(validate(scenario).has_value());

  scenario.scope = BenchmarkScope::storage;
  scenario.durability = static_cast<DurabilityLabel>(255U);
  EXPECT_TRUE(validate(scenario).has_value());
}

TEST(BenchmarkScenarioTest, SelectorRealizesExactMixAndIsRepeatable) {
  OperationSelector first({.reads = 95U, .writes = 5U}, 160016U, 7U);
  OperationSelector second({.reads = 95U, .writes = 5U}, 160016U, 7U);
  std::size_t reads = 0U;
  for (std::size_t index = 0; index < 1'000U; ++index) {
    const auto operation = first.next();
    EXPECT_EQ(operation, second.next());
    reads += operation == Operation::get ? 1U : 0U;
  }
  EXPECT_EQ(reads, 950U);
}

TEST(LatencyHistogramTest, MergesWithBoundedQuantileError) {
  LatencyHistogram left;
  LatencyHistogram right;
  for (std::int64_t us = 1; us <= 10'000; ++us) {
    (us % 2 == 0 ? left : right).observe(std::chrono::microseconds(us));
  }
  left.merge(right);

  EXPECT_EQ(left.count(), 10'000U);
  const auto p99 = left.quantile(0.99);
  ASSERT_TRUE(p99.has_value());
  EXPECT_NEAR(static_cast<double>(p99->count()), 9'900.0, 99.0);
  EXPECT_TRUE(left.quantile(0.999).has_value());
  EXPECT_EQ(left.maximum(), 10'000us);
}

TEST(LatencyHistogramTest, RejectsInvalidQuantilesAndCountsRangeOverflow) {
  LatencyHistogram histogram;
  histogram.observe(0us);
  histogram.observe(-1us);
  histogram.observe(60s);
  histogram.observe(60s + 1us);

  EXPECT_EQ(histogram.count(), 1U);
  EXPECT_EQ(histogram.underflow_count(), 2U);
  EXPECT_EQ(histogram.overflow_count(), 1U);
  EXPECT_FALSE(histogram.quantile(0.0).has_value());
  EXPECT_FALSE(histogram.quantile(1.01).has_value());
}

}  // namespace
}  // namespace forgekv::benchmarking
