#pragma once

#include "chaos/types.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <string>
#include <vector>

namespace forgekv::chaos {

struct HarnessOptions final {
  std::size_t node_count{3U};
  std::size_t client_count{4U};
  std::chrono::milliseconds duration{std::chrono::seconds(10)};
  std::chrono::milliseconds action_interval{std::chrono::seconds(1)};
  std::uint64_t seed{1U};
  std::filesystem::path server_path;
  std::filesystem::path artifact_directory;
  std::chrono::milliseconds overall_timeout{std::chrono::seconds(90)};
  bool keep_success{true};
  std::vector<ChaosAction> script;
  std::function<bool()> interrupted;
  bool enable_chaos{true};
  std::chrono::milliseconds request_timeout{std::chrono::milliseconds(250)};
};

struct HarnessSummary final {
  std::uint64_t seed{};
  std::uint64_t attempts{};
  std::uint64_t acknowledged_writes{};
  std::uint64_t actions{};
  bool converged{};
  bool restart_verified{};
  std::string failure_category;
  std::string first_evidence;
};

struct HarnessResult final {
  HarnessSummary summary;
  std::filesystem::path artifact_directory;
  std::string diagnostic;
  [[nodiscard]] bool ok() const noexcept { return diagnostic.empty(); }
};

class ChaosHarness final {
 public:
  explicit ChaosHarness(HarnessOptions options);
  [[nodiscard]] HarnessResult run();

 private:
  HarnessOptions options_;
};

}  // namespace forgekv::chaos
