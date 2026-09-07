#include "chaos/netem.h"

#include <cstdint>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace forgekv::chaos {
namespace {

TEST(NetemTest, RequiredProfilesUseExactLatencyAndLossMatrix) {
  const auto profiles = required_netem_profiles();
  ASSERT_EQ(profiles.size(), 7U);
  EXPECT_EQ(profiles[0], (NetemProfile{.name = "baseline"}));
  EXPECT_EQ(profiles[1],
            (NetemProfile{.name = "latency-10ms", .delay_us = 10'000U}));
  EXPECT_EQ(profiles[2],
            (NetemProfile{.name = "latency-50ms", .delay_us = 50'000U}));
  EXPECT_EQ(profiles[3],
            (NetemProfile{.name = "latency-100ms", .delay_us = 100'000U}));
  EXPECT_EQ(profiles[4],
            (NetemProfile{.name = "loss-0.1pct", .loss_basis_points = 10U}));
  EXPECT_EQ(profiles[5],
            (NetemProfile{.name = "loss-1pct", .loss_basis_points = 100U}));
  EXPECT_EQ(profiles[6],
            (NetemProfile{.name = "loss-5pct", .loss_basis_points = 500U}));
}

TEST(NetemTest, FormatsFixedArgumentVectorWithoutShellText) {
  const NetemProfile profile{
      .name = "combined",
      .delay_us = 10'000U,
      .jitter_us = 2'500U,
      .loss_basis_points = 10U,
      .reorder_basis_points = 100U,
      .correlation_basis_points = 2'500U,
  };
  const auto result = make_netem_arguments(profile);
  ASSERT_TRUE(result.ok()) << result.error;
  EXPECT_EQ(result.arguments,
            (std::vector<std::string>{"tc", "qdisc", "replace", "dev", "lo",
                                      "root", "netem", "delay", "10ms",
                                      "2500us", "loss", "0.1%", "reorder",
                                      "1%", "25%"}));
  EXPECT_EQ(netem_fault_label(profile), "packet impairment");
}

TEST(NetemTest, BaselineHasNoQdiscAndSemanticLabelsStayDistinct) {
  const NetemProfile baseline{.name = "baseline"};
  const auto result = make_netem_arguments(baseline);
  ASSERT_TRUE(result.ok());
  EXPECT_TRUE(result.arguments.empty());
  EXPECT_EQ(netem_fault_label(baseline), "none");
  EXPECT_EQ(proxy_fault_label(false), "connection reset");
  EXPECT_EQ(proxy_fault_label(true), "network partition");
}

TEST(NetemTest, SupportsJitterAndReorderingSmokeProfiles) {
  const auto jitter = named_netem_profile("jitter-smoke");
  const auto reorder = named_netem_profile("reorder-smoke");
  ASSERT_TRUE(jitter.has_value());
  ASSERT_TRUE(reorder.has_value());
  EXPECT_GT(jitter->jitter_us, 0U);
  EXPECT_GT(reorder->reorder_basis_points, 0U);
  EXPECT_GT(reorder->delay_us, 0U);
}

TEST(NetemTest, RejectsInvalidNamesAndNumericBounds) {
  EXPECT_FALSE(named_netem_profile("../../host").has_value());
  EXPECT_FALSE(make_netem_arguments(
                   NetemProfile{.name = "", .delay_us = 1U})
                   .ok());
  EXPECT_FALSE(make_netem_arguments(
                   NetemProfile{.name = "too-slow", .delay_us = 60'000'001U})
                   .ok());
  EXPECT_FALSE(make_netem_arguments(NetemProfile{
                                        .name = "bad-jitter",
                                        .jitter_us = 1U,
                                    })
                   .ok());
  EXPECT_FALSE(make_netem_arguments(NetemProfile{
                                        .name = "bad-loss",
                                        .loss_basis_points = 10'001U,
                                    })
                   .ok());
  EXPECT_FALSE(make_netem_arguments(NetemProfile{
                                        .name = "bad-reorder",
                                        .delay_us = 1U,
                                        .reorder_basis_points = 10'001U,
                                    })
                   .ok());
  EXPECT_FALSE(make_netem_arguments(NetemProfile{
                                        .name = "bad-correlation",
                                        .delay_us = 1U,
                                        .reorder_basis_points = 1U,
                                        .correlation_basis_points = 10'001U,
                                    })
                   .ok());
}

TEST(NetemTest, ParsesBoundedChaosSummaryAndQdiscCounters) {
  const auto summary = parse_chaos_output(
      "seed=15 artifacts=/tmp/a\n"
      "result=pass converged=true restart_verified=true attempts=321 "
      "acknowledged_writes=99 actions=0\n");
  ASSERT_TRUE(summary.ok()) << summary.error;
  EXPECT_TRUE(summary.summary->passed);
  EXPECT_TRUE(summary.summary->converged);
  EXPECT_TRUE(summary.summary->restart_verified);
  EXPECT_EQ(summary.summary->attempts, 321U);
  EXPECT_EQ(summary.summary->acknowledged_writes, 99U);
  EXPECT_EQ(summary.summary->actions, 0U);

  const auto qdisc = parse_qdisc_stats(
      "qdisc netem 8001: root refcnt 2 limit 1000 loss 1%\n"
      " Sent 1200 bytes 12 pkt (dropped 3, overlimits 0 requeues 0)\n");
  ASSERT_TRUE(qdisc.ok()) << qdisc.error;
  EXPECT_EQ(qdisc.stats->packets, 12U);
  EXPECT_EQ(qdisc.stats->dropped, 3U);
}

TEST(NetemTest, RejectsMalformedOrOversizedToolOutput) {
  EXPECT_FALSE(parse_chaos_output("result=pass attempts=nope").ok());
  EXPECT_FALSE(parse_qdisc_stats("qdisc netem but no counters").ok());
  EXPECT_FALSE(parse_chaos_output(std::string(65U * 1024U, 'x')).ok());
  EXPECT_FALSE(parse_qdisc_stats(std::string(65U * 1024U, 'x')).ok());
}

}  // namespace
}  // namespace forgekv::chaos
