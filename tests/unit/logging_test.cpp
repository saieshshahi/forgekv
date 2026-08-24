#include "common/logging.h"

#include <gtest/gtest.h>

#include <string>
#include <utility>
#include <vector>

namespace forgekv::common {
namespace {

class LoggerTest : public ::testing::Test {
 protected:
  void TearDown() override {
    Logger::set_runtime_minimum(Severity::info);
    Logger::reset_sink();
  }
};

TEST_F(LoggerTest, SeverityNamesAreStableForOperatorOutput) {
  EXPECT_EQ(severity_name(Severity::trace), "TRACE");
  EXPECT_EQ(severity_name(Severity::debug), "DEBUG");
  EXPECT_EQ(severity_name(Severity::info), "INFO");
  EXPECT_EQ(severity_name(Severity::warning), "WARN");
  EXPECT_EQ(severity_name(Severity::error), "ERROR");
  EXPECT_EQ(severity_name(Severity::critical), "CRITICAL");
  EXPECT_EQ(severity_name(Severity::off), "OFF");
}

TEST_F(LoggerTest, RuntimeMinimumFiltersLowerSeverities) {
  Logger::set_runtime_minimum(Severity::warning);

  EXPECT_FALSE(Logger::enabled(Severity::debug));
  EXPECT_FALSE(Logger::enabled(Severity::info));
  EXPECT_TRUE(Logger::enabled(Severity::warning));
  EXPECT_TRUE(Logger::enabled(Severity::error));
}

TEST_F(LoggerTest, SinkReceivesEnabledSeverityAndMessage) {
  std::vector<std::pair<Severity, std::string>> entries;
  Logger::set_runtime_minimum(Severity::info);
  Logger::set_sink([&entries](const Severity severity, const std::string_view message) {
    entries.emplace_back(severity, message);
  });

  FORGEKV_LOG(Severity::debug, "hidden");
  FORGEKV_LOG(Severity::error, "disk failed");

  ASSERT_EQ(entries.size(), 1U);
  EXPECT_EQ(entries.front().first, Severity::error);
  EXPECT_EQ(entries.front().second, "disk failed");
}

TEST_F(LoggerTest, OffDisablesEveryMessage) {
  Logger::set_runtime_minimum(Severity::off);

  EXPECT_FALSE(Logger::enabled(Severity::critical));
  EXPECT_FALSE(Logger::enabled(Severity::off));
}

}  // namespace
}  // namespace forgekv::common
