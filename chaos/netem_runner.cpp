#include "chaos/netem_runner.h"

#include <algorithm>
#include <chrono>
#include <cctype>
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
                                      std::vector<std::string> command) {
  std::vector<std::string> result{"ip", "netns", "exec", name};
  result.insert(result.end(), std::make_move_iterator(command.begin()),
                std::make_move_iterator(command.end()));
  return result;
}

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
  for (std::size_t index = 0U; index < options.profiles.size(); ++index) {
    NetemProfileResult profile_result;
    profile_result.profile = options.profiles[index];
    const auto netem = make_netem_arguments(profile_result.profile);
    if (!netem.ok()) {
      matrix.error = netem.error;
      return matrix;
    }
    const auto namespace_name = "fkv-netem-" + std::to_string(process_tag_) +
                                "-" + std::to_string(index);
    if (!valid_netem_namespace_name(namespace_name) ||
        !netem_interface_is_safe("lo")) {
      matrix.error = "unsafe netem namespace target";
      return matrix;
    }

    bool owns_namespace = false;
    auto cleanup = [&] {
      if (!owns_namespace) {
        CommandResult already_clean;
        already_clean.exit_code = 0;
        return already_clean;
      }
      owns_namespace = false;
      return executor_.run({"ip", "netns", "del", namespace_name},
                           setup_options);
    };
    const auto fail = [&](const std::string& diagnostic) {
      profile_result.error = diagnostic;
      const auto cleaned = cleanup();
      if (!cleaned.ok()) {
        profile_result.error += "; " + command_failure("namespace cleanup",
                                                        cleaned);
      }
      matrix.profiles.push_back(std::move(profile_result));
      matrix.error = matrix.profiles.back().error;
    };

    const auto created = executor_.run(
        {"ip", "netns", "add", namespace_name}, setup_options);
    if (!created.ok()) {
      fail(command_failure("namespace create", created));
      return matrix;
    }
    owns_namespace = true;
    const auto loopback = executor_.run(
        in_namespace(namespace_name, {"ip", "link", "set", "lo", "up"}),
        setup_options);
    if (!loopback.ok()) {
      fail(command_failure("enable isolated loopback", loopback));
      return matrix;
    }
    if (!netem.arguments.empty()) {
      const auto applied = executor_.run(
          in_namespace(namespace_name, netem.arguments), setup_options);
      if (!applied.ok()) {
        fail(command_failure("apply netem profile", applied));
        return matrix;
      }
    }

    std::vector<std::string> workload{
        options.chaos_path.string(),
        "--nodes", std::to_string(options.nodes),
        "--clients", std::to_string(options.clients),
        "--duration", std::to_string(options.duration.count()),
        "--seed", std::to_string(options.seed),
        "--server", options.server_path.string(),
        "--artifacts",
        (options.output_directory / profile_result.profile.name).string(),
        "--keep-success", "--no-chaos",
    };
    const auto started = Clock::now();
    const auto workload_result = executor_.run(
        in_namespace(namespace_name, std::move(workload)),
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
      return matrix;
    }
    profile_result.summary = *summary.summary;

    if (!netem.arguments.empty()) {
      const auto stats_result = executor_.run(
          in_namespace(namespace_name,
                       {"tc", "-s", "qdisc", "show", "dev", "lo"}),
          setup_options);
      const auto stats = parse_qdisc_stats(stats_result.output);
      if (!stats_result.ok() || !stats.ok()) {
        fail(command_failure("collect qdisc statistics", stats_result) +
             (stats.ok() ? "" : "; " + stats.error));
        return matrix;
      }
      profile_result.qdisc = *stats.stats;
    }

    const auto cleaned = cleanup();
    if (!cleaned.ok()) {
      profile_result.error = command_failure("namespace cleanup", cleaned);
    } else if (!workload_result.ok() || !profile_result.summary.passed) {
      profile_result.error = command_failure("stable ForgeKV workload",
                                             workload_result);
    }
    matrix.profiles.push_back(std::move(profile_result));
    if (!matrix.profiles.back().ok()) {
      matrix.error = matrix.profiles.back().error;
      return matrix;
    }
  }
  return matrix;
}

}  // namespace forgekv::chaos
