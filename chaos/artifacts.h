#pragma once

#include "chaos/client_worker.h"
#include "chaos/types.h"

#include <cstddef>
#include <filesystem>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace forgekv::chaos {

struct ArtifactLimits final {
  std::size_t maximum_records{1'000'000U};
  std::size_t maximum_bytes{256U * 1024U * 1024U};
  std::size_t maximum_line_bytes{64U * 1024U};
};

struct ArtifactStatus final {
  std::string error;
  [[nodiscard]] bool ok() const noexcept { return error.empty(); }
};

struct TimelineResult final {
  std::vector<ChaosAction> actions;
  std::string error;
  [[nodiscard]] bool ok() const noexcept { return error.empty(); }
};

struct CampaignConfig final {
  std::size_t nodes{};
  std::size_t clients{};
  std::uint64_t duration_ms{};
  std::uint64_t action_interval_ms{};
  std::uint64_t seed{};
  bool chaos_enabled{true};
  std::uint64_t request_timeout_ms{250U};
};

struct CampaignConfigResult final {
  std::optional<CampaignConfig> config;
  std::string error;
  [[nodiscard]] bool ok() const noexcept { return config.has_value(); }
};

struct AttemptRecord final {
  std::string client;
  LogicalRequest request;
  Endpoint endpoint;
  AttemptKind result{AttemptKind::transport_error};
  std::string diagnostic;
  std::uint64_t observed_start_us{};
  std::uint64_t observed_finish_us{};
};

[[nodiscard]] std::string json_string(std::string_view value);
[[nodiscard]] std::string_view action_kind_name(ActionKind kind) noexcept;
[[nodiscard]] TimelineResult read_timeline(
    const std::filesystem::path& path,
    ArtifactLimits limits = ArtifactLimits{});
[[nodiscard]] CampaignConfigResult read_campaign_config(
    const std::filesystem::path& path);

class ArtifactWriter final {
 public:
  ArtifactWriter(std::filesystem::path directory, ArtifactLimits limits);

  [[nodiscard]] ArtifactStatus append_action(const ChaosAction& action,
                                             std::uint64_t observed_start_us,
                                             std::uint64_t observed_finish_us);
  [[nodiscard]] ArtifactStatus append_attempt(const AttemptRecord& attempt);
  [[nodiscard]] ArtifactStatus publish(std::string_view filename,
                                       std::string_view contents) const;

 private:
  std::filesystem::path directory_;
  ArtifactLimits limits_;
  std::size_t timeline_records_{};
  std::size_t timeline_bytes_{};
  std::size_t history_records_{};
  std::size_t history_bytes_{};
  mutable std::mutex mutex_;
};

}  // namespace forgekv::chaos
