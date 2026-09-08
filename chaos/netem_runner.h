#pragma once

#include "chaos/netem.h"
#include "chaos/process_runner.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <string>
#include <string_view>
#include <vector>

namespace forgekv::chaos {

struct NetemRunnerOptions final {
  std::filesystem::path chaos_path;
  std::filesystem::path server_path;
  std::filesystem::path output_directory;
  std::vector<NetemProfile> profiles;
  std::size_t nodes{3U};
  std::size_t clients{4U};
  std::chrono::seconds duration{std::chrono::seconds(10)};
  std::uint64_t seed{1U};
  std::function<bool()> interrupted;
};

struct NetemProfileResult final {
  NetemProfile profile;
  ChaosRunSummary summary;
  QdiscStats qdisc;
  std::chrono::milliseconds wall_duration{};
  int workload_exit_code{-1};
  std::string workload_output;
  std::string qdisc_output;
  std::string error;
  [[nodiscard]] bool stable_contract_met() const noexcept {
    return summary.passed && summary.actions == 0U && summary.converged &&
           summary.restart_verified;
  }
  [[nodiscard]] bool ok() const noexcept {
    return error.empty() && workload_exit_code == 0 && stable_contract_met();
  }
};

struct NetemMatrixResult final {
  std::vector<NetemProfileResult> profiles;
  std::string error;
  [[nodiscard]] bool ok() const noexcept { return error.empty(); }
};

[[nodiscard]] bool valid_netem_namespace_name(std::string_view name);
[[nodiscard]] bool netem_interface_is_safe(std::string_view interface_name);

class NetemRunner final {
 public:
  NetemRunner(CommandExecutor& executor, std::uint32_t effective_uid,
              std::uint64_t process_tag);
  [[nodiscard]] NetemMatrixResult run(const NetemRunnerOptions& options);

 private:
  CommandExecutor& executor_;
  std::uint32_t effective_uid_{};
  std::uint64_t process_tag_{};
};

}  // namespace forgekv::chaos
