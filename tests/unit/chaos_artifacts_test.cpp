#include "chaos/artifacts.h"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include <unistd.h>

namespace forgekv::chaos {
namespace {

class TemporaryDirectory final {
 public:
  TemporaryDirectory() {
    static std::atomic<std::uint64_t> sequence{};
    path_ = std::filesystem::temp_directory_path() /
            ("forgekv-chaos-artifacts-" + std::to_string(::getpid()) + "-" +
             std::to_string(sequence.fetch_add(1U)));
    std::filesystem::create_directories(path_);
  }
  ~TemporaryDirectory() { std::filesystem::remove_all(path_); }
  const std::filesystem::path& path() const noexcept { return path_; }

 private:
  std::filesystem::path path_;
};

std::string read_text(const std::filesystem::path& path) {
  std::ifstream input(path, std::ios::binary);
  return {std::istreambuf_iterator<char>(input),
          std::istreambuf_iterator<char>()};
}

TEST(ChaosArtifactsTest, EscapesJsonControlCharacters) {
  EXPECT_EQ(json_string("line\n\"quoted\"\\tail\t"),
            "\"line\\n\\\"quoted\\\"\\\\tail\\t\"");
  EXPECT_EQ(json_string(std::string(1U, static_cast<char>(1))), "\"\\u0001\"");
}

TEST(ChaosArtifactsTest, AppendsAndStrictlyRoundTripsRealizedTimeline) {
  TemporaryDirectory directory;
  ArtifactWriter writer(directory.path(), ArtifactLimits{});
  const ChaosAction first{.kind = ActionKind::set_latency,
                          .planned_offset_us = 250'000U,
                          .node = 1U,
                          .peer = 3U,
                          .value = 50U};
  const ChaosAction second{.kind = ActionKind::kill_leader,
                           .planned_offset_us = 500'000U,
                           .node = 2U};

  ASSERT_TRUE(writer.append_action(first, 251'000U, 252'000U).ok());
  ASSERT_TRUE(writer.append_action(second, 501'000U, 503'000U).ok());
  const auto replay = read_timeline(directory.path() / "timeline.jsonl");

  ASSERT_TRUE(replay.ok()) << replay.error;
  EXPECT_EQ(replay.actions, (std::vector<ChaosAction>{first, second}));
  EXPECT_NE(read_text(directory.path() / "timeline.jsonl")
                .find("\"observed_start_us\":251000"),
            std::string::npos);
}

TEST(ChaosArtifactsTest, RejectsMalformedUnknownAndOversizedReplay) {
  TemporaryDirectory directory;
  {
    std::ofstream output(directory.path() / "timeline.jsonl");
    output << "{\"kind\":\"invented\"}\n";
  }
  EXPECT_FALSE(read_timeline(directory.path() / "timeline.jsonl").ok());

  {
    ArtifactWriter complete(directory.path() / "truncated", ArtifactLimits{});
    ASSERT_TRUE(complete.append_action(
                            ChaosAction{.kind = ActionKind::heal_network},
                            1U, 2U)
                    .ok());
    const auto path = directory.path() / "truncated" / "timeline.jsonl";
    std::filesystem::resize_file(path, std::filesystem::file_size(path) - 1U);
    EXPECT_FALSE(read_timeline(path).ok());
  }

  {
    std::ofstream output(directory.path() / "timeline.jsonl",
                         std::ios::trunc);
    output << "{\"kind\":\"kill_leader\"";
  }
  EXPECT_FALSE(read_timeline(directory.path() / "timeline.jsonl").ok());

  std::filesystem::resize_file(directory.path() / "timeline.jsonl",
                               ArtifactLimits{}.maximum_bytes + 1U);
  EXPECT_FALSE(read_timeline(directory.path() / "timeline.jsonl").ok());

  ArtifactLimits limits;
  limits.maximum_records = 1U;
  ArtifactWriter writer(directory.path() / "bounded", limits);
  const ChaosAction action{.kind = ActionKind::heal_network};
  EXPECT_TRUE(writer.append_action(action, 1U, 2U).ok());
  EXPECT_FALSE(writer.append_action(action, 3U, 4U).ok());
}

TEST(ChaosArtifactsTest, RejectsAReusedNonemptyArtifactDirectory) {
  TemporaryDirectory directory;
  {
    std::ofstream marker(directory.path() / "existing.txt");
    marker << "do not mix campaigns";
  }
  EXPECT_THROW(ArtifactWriter(directory.path(), ArtifactLimits{}),
               std::invalid_argument);
}

TEST(ChaosArtifactsTest, AtomicallyPublishesNamedFiles) {
  TemporaryDirectory directory;
  ArtifactWriter writer(directory.path(), ArtifactLimits{});
  ASSERT_TRUE(writer.publish("summary.json", "{\"result\":\"pass\"}\n").ok());
  EXPECT_EQ(read_text(directory.path() / "summary.json"),
            "{\"result\":\"pass\"}\n");
  EXPECT_FALSE(std::filesystem::exists(directory.path() / "summary.json.tmp"));
  EXPECT_FALSE(writer.publish("../escape", "bad").ok());
}

TEST(ChaosArtifactsTest, ReadsStrictCampaignConfigAndRejectsOversizedInput) {
  TemporaryDirectory directory;
  ArtifactWriter writer(directory.path(), ArtifactLimits{});
  ASSERT_TRUE(writer.publish(
                        "config.json",
                        "{\"version\":1,\"nodes\":5,\"clients\":32,"
                        "\"duration_ms\":120000,\"action_interval_ms\":500,"
                        "\"seed\":12345}\n")
                  .ok());
  const auto config = read_campaign_config(directory.path() / "config.json");
  ASSERT_TRUE(config.ok()) << config.error;
  EXPECT_EQ(config.config->nodes, 5U);
  EXPECT_EQ(config.config->clients, 32U);
  EXPECT_EQ(config.config->seed, 12345U);
  EXPECT_TRUE(config.config->chaos_enabled);
  EXPECT_EQ(config.config->request_timeout_ms, 250U);

  ASSERT_TRUE(writer.publish(
                        "config-v2.json",
                        "{\"version\":1,\"nodes\":3,\"clients\":8,"
                        "\"duration_ms\":3000,\"action_interval_ms\":1000,"
                        "\"seed\":150015,\"chaos_enabled\":false,"
                        "\"request_timeout_ms\":1050}\n")
                  .ok());
  const auto extended =
      read_campaign_config(directory.path() / "config-v2.json");
  ASSERT_TRUE(extended.ok()) << extended.error;
  EXPECT_FALSE(extended.config->chaos_enabled);
  EXPECT_EQ(extended.config->request_timeout_ms, 1050U);

  std::filesystem::resize_file(directory.path() / "config.json", 4097U);
  EXPECT_FALSE(read_campaign_config(directory.path() / "config.json").ok());
}

TEST(ChaosArtifactsTest, AttemptHistoryContainsReplayDiagnostics) {
  TemporaryDirectory directory;
  ArtifactWriter writer(directory.path(), ArtifactLimits{});
  ClientId client_id{};
  client_id[0] = std::byte{0xab};
  ASSERT_TRUE(writer.append_attempt(AttemptRecord{
                         .client = "client-1",
                         .request = LogicalRequest{.operation = ClientOperation::put,
                                                   .client_id = client_id,
                                                   .request_id = 7U,
                                                   .key = "owned-key",
                                                   .value = "owned-value"},
                         .endpoint = Endpoint{.port = 7001U},
                         .result = AttemptKind::timeout,
                         .diagnostic = "deadline",
                         .observed_start_us = 10U,
                         .observed_finish_us = 20U})
                  .ok());
  const auto history = read_text(directory.path() / "history.jsonl");
  EXPECT_NE(history.find("\"sequence\":1"), std::string::npos);
  EXPECT_NE(history.find("ab000000000000000000000000000000"),
            std::string::npos);
  EXPECT_NE(history.find("\"key\":\"owned-key\""), std::string::npos);
  EXPECT_NE(history.find("\"value\":\"owned-value\""), std::string::npos);
  EXPECT_NE(history.find("\"diagnostic\":\"deadline\""),
            std::string::npos);
}

}  // namespace
}  // namespace forgekv::chaos
