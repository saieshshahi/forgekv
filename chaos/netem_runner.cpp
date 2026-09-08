#include "chaos/netem_runner.h"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <exception>
#include <string>
#include <utility>
#include <vector>

namespace forgekv::chaos {
namespace {

using Clock = std::chrono::steady_clock;

std::string command_failure(const std::string_view action,
                            const CommandResult& result) {
  auto diagnostic = std::string(action) + " failed";
  if (!result.error.empty()) diagnostic += ": " + result.error;
  if (!result.output.empty()) diagnostic += ": " + result.output;
  return diagnostic;
}

std::vector<std::string> in_namespace(const std::string& name,
                                      const std::filesystem::path& ip_path,
                                      std::vector<std::string> command) {
  std::vector<std::string> result{ip_path.string(), "netns", "exec", name};
  result.insert(result.end(), std::make_move_iterator(command.begin()),
                std::make_move_iterator(command.end()));
  return result;
}

std::uint64_t request_timeout_ms(const NetemProfile& profile) {
  const auto network_delay_ms =
      (static_cast<std::uint64_t>(profile.delay_us) + profile.jitter_us + 999U) /
      1000U;
  auto timeout = std::max<std::uint64_t>(250U, 250U + 8U * network_delay_ms);
  if (profile.loss_basis_points != 0U ||
      profile.reorder_basis_points != 0U) {
    timeout = std::max<std::uint64_t>(timeout, 2'000U);
  }
  return std::min<std::uint64_t>(timeout, 10'000U);
}

class NamespaceGuard final {
 public:
  NamespaceGuard(CommandExecutor& executor, std::string name,
                 std::filesystem::path ip_path,
                 CommandOptions cleanup_options)
      : executor_(executor),
        name_(std::move(name)),
        ip_path_(std::move(ip_path)),
        cleanup_options_(std::move(cleanup_options)) {}

  ~NamespaceGuard() { static_cast<void>(cleanup()); }

  NamespaceGuard(const NamespaceGuard&) = delete;
  NamespaceGuard& operator=(const NamespaceGuard&) = delete;

  void take_ownership() noexcept { owns_namespace_ = true; }

  [[nodiscard]] CommandResult cleanup() noexcept {
    CommandResult result;
    result.exit_code = 0;
    if (!owns_namespace_) return result;
    owns_namespace_ = false;
    try {
      return executor_.run(
          {ip_path_.string(), "netns", "del", name_}, cleanup_options_);
    } catch (const std::exception& error) {
      result.exit_code = -1;
      result.error = std::string("namespace cleanup exception: ") + error.what();
    } catch (...) {
      result.exit_code = -1;
      result.error = "namespace cleanup exception";
    }
    return result;
  }

 private:
  CommandExecutor& executor_;
  std::string name_;
  std::filesystem::path ip_path_;
  CommandOptions cleanup_options_;
  bool owns_namespace_{false};
};

}  // namespace

bool valid_netem_namespace_name(const std::string_view name) {
  constexpr std::string_view prefix{"fkv-netem-"};
  if (!name.starts_with(prefix) || name.size() <= prefix.size() ||
      name.size() > 63U) {
    return false;
  }
  return std::ranges::all_of(name.substr(prefix.size()),
                             [](const unsigned char character) {
                               return std::isalnum(character) != 0 ||
                                      character == '-';
                             });
}

bool netem_interface_is_safe(const std::string_view interface_name) {
  return interface_name == "lo";
}

NetemRunner::NetemRunner(CommandExecutor& executor,
                         const std::uint32_t effective_uid,
                         const std::uint64_t process_tag)
    : executor_(executor),
      effective_uid_(effective_uid),
      process_tag_(process_tag) {}

NetemMatrixResult NetemRunner::run(const NetemRunnerOptions& options) {
  NetemMatrixResult matrix;
  if (effective_uid_ != 0U) {
    matrix.error = "forgekv-netem requires root for isolated namespaces";
    return matrix;
  }
  if (options.chaos_path.empty() || options.server_path.empty() ||
      options.output_directory.empty() || options.profiles.empty() ||
      !options.ip_path.is_absolute() || !options.tc_path.is_absolute() ||
      (options.nodes != 3U && options.nodes != 5U) || options.clients == 0U ||
      options.clients > 256U || options.duration.count() <= 0 ||
      options.duration > std::chrono::hours(1)) {
    matrix.error = "invalid netem runner options";
    return matrix;
  }

  const CommandOptions setup_options{
      .timeout = std::chrono::seconds(10),
      .maximum_output_bytes = 64U * 1024U,
      .interrupted = options.interrupted,
  };
  const CommandOptions lifecycle_options{
      .timeout = std::chrono::seconds(10),
      .maximum_output_bytes = 64U * 1024U,
      .interrupted = {},
  };
  for (std::size_t index = 0U; index < options.profiles.size(); ++index) {
    NetemProfileResult profile_result;
    profile_result.profile = options.profiles[index];
    auto netem = make_netem_arguments(profile_result.profile);
    if (!netem.ok()) {
      matrix.error = netem.error;
      return matrix;
    }
    if (!netem.arguments.empty()) {
      netem.arguments.front() = options.tc_path.string();
    }
    const auto namespace_name = "fkv-netem-" + std::to_string(process_tag_) +
                                "-" + std::to_string(index);
    if (!valid_netem_namespace_name(namespace_name) ||
        !netem_interface_is_safe("lo")) {
      matrix.error = "unsafe netem namespace target";
      return matrix;
    }

    NamespaceGuard namespace_guard(executor_, namespace_name, options.ip_path,
                                   lifecycle_options);
    const auto fail = [&](const std::string& diagnostic) {
      profile_result.error = diagnostic;
      const auto cleaned = namespace_guard.cleanup();
      if (!cleaned.ok()) {
        profile_result.error += "; " + command_failure("namespace cleanup",
                                                        cleaned);
      }
      matrix.profiles.push_back(std::move(profile_result));
      if (matrix.error.empty()) matrix.error = matrix.profiles.back().error;
    };

    try {
      const auto created = executor_.run(
          {options.ip_path.string(), "netns", "add", namespace_name},
          lifecycle_options);
      if (!created.ok()) {
        fail(command_failure("namespace create", created));
        if (options.interrupted && options.interrupted()) return matrix;
        continue;
      }
      namespace_guard.take_ownership();
      const auto loopback = executor_.run(
          in_namespace(namespace_name, options.ip_path,
                       {options.ip_path.string(), "link", "set", "lo", "up"}),
          setup_options);
      if (!loopback.ok()) {
        fail(command_failure("enable isolated loopback", loopback));
        if (options.interrupted && options.interrupted()) return matrix;
        continue;
      }
      if (!netem.arguments.empty()) {
        const auto applied = executor_.run(
            in_namespace(namespace_name, options.ip_path, netem.arguments),
            setup_options);
        if (!applied.ok()) {
          fail(command_failure("apply netem profile", applied));
          if (options.interrupted && options.interrupted()) return matrix;
          continue;
        }
      }

      std::vector<std::string> workload{
          options.chaos_path.string(),
          "--nodes", std::to_string(options.nodes),
          "--clients", std::to_string(options.clients),
          "--duration", std::to_string(options.duration.count()),
          "--request-timeout-ms",
          std::to_string(request_timeout_ms(profile_result.profile)),
          "--seed", std::to_string(options.seed),
          "--server", options.server_path.string(),
          "--artifacts",
          (options.output_directory / profile_result.profile.name).string(),
          "--keep-success", "--no-chaos",
      };
      const auto started = Clock::now();
      const auto workload_result = executor_.run(
          in_namespace(namespace_name, options.ip_path, std::move(workload)),
          CommandOptions{
              .timeout = options.duration + std::chrono::seconds(120),
              .maximum_output_bytes = 64U * 1024U,
              .interrupted = options.interrupted,
          });
      profile_result.wall_duration =
          std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() -
                                                                started);
      profile_result.workload_exit_code = workload_result.exit_code;
      profile_result.workload_output = workload_result.output;
      const auto summary = parse_chaos_output(workload_result.output);
      if (!summary.ok()) {
        fail(command_failure("stable ForgeKV workload", workload_result) +
             "; " + summary.error);
        if (options.interrupted && options.interrupted()) return matrix;
        continue;
      }
      profile_result.summary = *summary.summary;

      if (!netem.arguments.empty()) {
        const auto stats_result = executor_.run(
            in_namespace(namespace_name, options.ip_path,
                         {options.tc_path.string(), "-s", "qdisc", "show",
                          "dev", "lo"}),
            setup_options);
        const auto stats = parse_qdisc_stats(stats_result.output);
        profile_result.qdisc_output = stats_result.output;
        if (!stats_result.ok() || !stats.ok()) {
          fail(command_failure("collect qdisc statistics", stats_result) +
               (stats.ok() ? "" : "; " + stats.error));
          if (options.interrupted && options.interrupted()) return matrix;
          continue;
        }
        profile_result.qdisc = *stats.stats;
      }

      const auto cleaned = namespace_guard.cleanup();
      if (!cleaned.ok()) {
        profile_result.error = command_failure("namespace cleanup", cleaned);
      } else if (!workload_result.ok() ||
                 !profile_result.stable_contract_met()) {
        profile_result.error = command_failure("stable ForgeKV workload",
                                               workload_result);
      }
      matrix.profiles.push_back(std::move(profile_result));
      if (!matrix.profiles.back().ok()) {
        if (matrix.error.empty()) matrix.error = matrix.profiles.back().error;
      }
    } catch (const std::exception& error) {
      fail(std::string("netem profile exception: ") + error.what());
      if (options.interrupted && options.interrupted()) return matrix;
    } catch (...) {
      fail("netem profile exception");
      if (options.interrupted && options.interrupted()) return matrix;
    }
  }
  return matrix;
}

}  // namespace forgekv::chaos
