#include "chaos/netem.h"

#include <algorithm>
#include <charconv>
#include <cctype>
#include <limits>
#include <sstream>
#include <string>
#include <utility>

namespace forgekv::chaos {
namespace {

constexpr std::size_t kMaximumToolOutput = 64U * 1024U;
constexpr std::uint32_t kMaximumDelayUs = 60'000'000U;
constexpr std::uint32_t kMaximumBasisPoints = 10'000U;

bool valid_name(const std::string_view name) {
  return !name.empty() && name.size() <= 64U &&
         std::ranges::all_of(name, [](const unsigned char character) {
           return std::islower(character) != 0 || std::isdigit(character) != 0 ||
                  character == '-' || character == '.';
         });
}

std::string duration_text(const std::uint32_t microseconds) {
  if (microseconds % 1000U == 0U) {
    return std::to_string(microseconds / 1000U) + "ms";
  }
  return std::to_string(microseconds) + "us";
}

std::string percent_text(const std::uint32_t basis_points) {
  const auto whole = basis_points / 100U;
  const auto remainder = basis_points % 100U;
  if (remainder == 0U) {
    return std::to_string(whole) + "%";
  }
  if (remainder % 10U == 0U) {
    return std::to_string(whole) + "." + std::to_string(remainder / 10U) + "%";
  }
  const auto tens = remainder / 10U;
  const auto ones = remainder % 10U;
  return std::to_string(whole) + "." + std::to_string(tens) +
         std::to_string(ones) + "%";
}

bool parse_unsigned(const std::string_view text, std::uint64_t& value) {
  const auto [end, error] =
      std::from_chars(text.data(), text.data() + text.size(), value);
  return !text.empty() && error == std::errc{} &&
         end == text.data() + text.size();
}

std::optional<std::string_view> field(const std::string_view line,
                                      const std::string_view name) {
  const auto marker = std::string(name) + "=";
  const auto position = line.find(marker);
  if (position == std::string_view::npos ||
      (position != 0U && !std::isspace(static_cast<unsigned char>(
                             line[position - 1U])))) {
    return std::nullopt;
  }
  const auto start = position + marker.size();
  const auto finish = line.find_first_of(" \r\n\t", start);
  return line.substr(start, finish == std::string_view::npos
                                ? line.size() - start
                                : finish - start);
}

}  // namespace

std::vector<NetemProfile> required_netem_profiles() {
  return {
      NetemProfile{.name = "baseline"},
      NetemProfile{.name = "latency-10ms", .delay_us = 10'000U},
      NetemProfile{.name = "latency-50ms", .delay_us = 50'000U},
      NetemProfile{.name = "latency-100ms", .delay_us = 100'000U},
      NetemProfile{.name = "loss-0.1pct", .loss_basis_points = 10U},
      NetemProfile{.name = "loss-1pct", .loss_basis_points = 100U},
      NetemProfile{.name = "loss-5pct", .loss_basis_points = 500U},
  };
}

std::optional<NetemProfile> named_netem_profile(const std::string_view name) {
  auto profiles = required_netem_profiles();
  profiles.push_back(NetemProfile{.name = "jitter-smoke",
                                  .delay_us = 10'000U,
                                  .jitter_us = 5'000U});
  profiles.push_back(NetemProfile{.name = "reorder-smoke",
                                  .delay_us = 10'000U,
                                  .reorder_basis_points = 100U,
                                  .correlation_basis_points = 2'500U});
  const auto found = std::ranges::find(profiles, name, &NetemProfile::name);
  return found == profiles.end() ? std::nullopt
                                 : std::optional<NetemProfile>(*found);
}

NetemArguments make_netem_arguments(const NetemProfile& profile) {
  if (!valid_name(profile.name)) {
    return {.arguments = {}, .error = "invalid netem profile name"};
  }
  if (profile.delay_us > kMaximumDelayUs ||
      profile.jitter_us > kMaximumDelayUs) {
    return {.arguments = {}, .error = "netem delay exceeds 60 seconds"};
  }
  if (profile.loss_basis_points > kMaximumBasisPoints ||
      profile.reorder_basis_points > kMaximumBasisPoints ||
      profile.correlation_basis_points > kMaximumBasisPoints) {
    return {.arguments = {}, .error = "netem percentage exceeds 100%"};
  }
  if (profile.jitter_us != 0U && profile.delay_us == 0U) {
    return {.arguments = {},
            .error = "netem jitter requires a base delay"};
  }
  if ((profile.reorder_basis_points != 0U ||
       profile.correlation_basis_points != 0U) &&
      profile.delay_us == 0U) {
    return {.arguments = {},
            .error = "netem reordering requires a base delay"};
  }
  if (profile.correlation_basis_points != 0U &&
      profile.reorder_basis_points == 0U) {
    return {.arguments = {},
            .error = "netem correlation requires reordering"};
  }

  NetemArguments result;
  if (profile.delay_us == 0U && profile.loss_basis_points == 0U &&
      profile.reorder_basis_points == 0U) {
    return result;
  }
  result.arguments = {"tc", "qdisc", "replace", "dev", "lo", "root", "netem"};
  if (profile.delay_us != 0U) {
    result.arguments.push_back("delay");
    result.arguments.push_back(duration_text(profile.delay_us));
    if (profile.jitter_us != 0U) {
      result.arguments.push_back(duration_text(profile.jitter_us));
    }
  }
  if (profile.loss_basis_points != 0U) {
    result.arguments.push_back("loss");
    result.arguments.push_back(percent_text(profile.loss_basis_points));
  }
  if (profile.reorder_basis_points != 0U) {
    result.arguments.push_back("reorder");
    result.arguments.push_back(percent_text(profile.reorder_basis_points));
    if (profile.correlation_basis_points != 0U) {
      result.arguments.push_back(percent_text(profile.correlation_basis_points));
    }
  }
  return result;
}

std::string netem_fault_label(const NetemProfile& profile) {
  return profile.delay_us == 0U && profile.jitter_us == 0U &&
                 profile.loss_basis_points == 0U &&
                 profile.reorder_basis_points == 0U
             ? "none"
             : "packet impairment";
}

std::string proxy_fault_label(const bool partitioned) {
  return partitioned ? "network partition" : "connection reset";
}

ChaosSummaryParse parse_chaos_output(const std::string_view output) {
  if (output.size() > kMaximumToolOutput) {
    return {.summary = std::nullopt,
            .error = "chaos output exceeds 64 KiB"};
  }
  const auto position = output.rfind("result=");
  if (position == std::string_view::npos ||
      (position != 0U && output[position - 1U] != '\n')) {
    return {.summary = std::nullopt,
            .error = "chaos result line is missing"};
  }
  const auto end = output.find('\n', position);
  const auto line = output.substr(position, end == std::string_view::npos
                                                ? output.size() - position
                                                : end - position);
  const auto result = field(line, "result");
  const auto converged = field(line, "converged");
  const auto restarted = field(line, "restart_verified");
  const auto attempts = field(line, "attempts");
  const auto acknowledged = field(line, "acknowledged_writes");
  const auto actions = field(line, "actions");
  if (!result || !converged || !restarted || !attempts || !acknowledged ||
      !actions || (*result != "pass" && *result != "fail") ||
      (*converged != "true" && *converged != "false") ||
      (*restarted != "true" && *restarted != "false")) {
    return {.summary = std::nullopt,
            .error = "malformed chaos result line"};
  }
  ChaosRunSummary summary{.passed = *result == "pass",
                          .converged = *converged == "true",
                          .restart_verified = *restarted == "true"};
  if (!parse_unsigned(*attempts, summary.attempts) ||
      !parse_unsigned(*acknowledged, summary.acknowledged_writes) ||
      !parse_unsigned(*actions, summary.actions)) {
    return {.summary = std::nullopt,
            .error = "malformed chaos result counter"};
  }
  return {.summary = summary, .error = {}};
}

QdiscStatsParse parse_qdisc_stats(const std::string_view output) {
  if (output.size() > kMaximumToolOutput) {
    return {.stats = std::nullopt,
            .error = "qdisc output exceeds 64 KiB"};
  }
  constexpr std::string_view sent_marker{" Sent "};
  constexpr std::string_view bytes_marker{" bytes "};
  constexpr std::string_view packet_marker{" pkt (dropped "};
  const auto sent = output.find(sent_marker);
  const auto bytes = sent == std::string_view::npos
                         ? std::string_view::npos
                         : output.find(bytes_marker, sent + sent_marker.size());
  const auto packets_start =
      bytes == std::string_view::npos ? std::string_view::npos
                                      : bytes + bytes_marker.size();
  const auto packets_end = packets_start == std::string_view::npos
                               ? std::string_view::npos
                               : output.find(packet_marker, packets_start);
  const auto dropped_start =
      packets_end == std::string_view::npos
          ? std::string_view::npos
          : packets_end + packet_marker.size();
  const auto dropped_end = dropped_start == std::string_view::npos
                               ? std::string_view::npos
                               : output.find(',', dropped_start);
  if (packets_end == std::string_view::npos ||
      dropped_end == std::string_view::npos) {
    return {.stats = std::nullopt, .error = "qdisc counters are missing"};
  }
  QdiscStats stats;
  if (!parse_unsigned(output.substr(packets_start, packets_end - packets_start),
                      stats.packets) ||
      !parse_unsigned(output.substr(dropped_start, dropped_end - dropped_start),
                      stats.dropped)) {
    return {.stats = std::nullopt, .error = "malformed qdisc counters"};
  }
  return {.stats = stats, .error = {}};
}

}  // namespace forgekv::chaos
