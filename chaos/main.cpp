#include "chaos/artifacts.h"
#include "chaos/harness.h"

#include <array>
#include <charconv>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>

namespace {

using forgekv::chaos::HarnessOptions;

volatile std::sig_atomic_t shutdown_requested = 0;

void request_shutdown(const int) { shutdown_requested = 1; }

void usage(std::ostream& output) {
  output <<
      "usage: forgekv-chaos --server PATH [options]\n"
      "  --nodes 3|5                     cluster size (default: 3)\n"
      "  --clients 1..256                clients (default: 4)\n"
      "  --duration 1..3600              seconds (default: 10)\n"
      "  --action-interval-ms 50..60000  interval (default: 1000)\n"
      "  --seed UINT64                   decision seed\n"
      "  --artifacts PATH                output directory\n"
      "  --replay TIMELINE               replay realized actions\n"
      "  --no-chaos                     stable workload without scheduled faults\n"
      "  --keep-success                  retain passing artifacts\n"
      "  --help\n";
}

std::uint64_t number(const std::string_view text, const char* name) {
  std::uint64_t value = 0U;
  const auto [end, error] =
      std::from_chars(text.data(), text.data() + text.size(), value);
  if (text.empty() || error != std::errc{} ||
      end != text.data() + text.size()) {
    throw std::invalid_argument(std::string("invalid ") + name);
  }
  return value;
}

std::string option_name(const std::string_view argument) {
  return std::string(argument.substr(0U, argument.find('=')));
}

std::optional<std::string> inline_value(const std::string_view argument) {
  const auto separator = argument.find('=');
  if (separator == std::string_view::npos) {
    return std::nullopt;
  }
  return std::string(argument.substr(separator + 1U));
}

struct Parsed final {
  HarnessOptions options;
  bool keep_success{};
};

Parsed parse(const int argc, char** argv) {
  Parsed parsed;
  std::set<std::string> seen;
  std::optional<std::filesystem::path> replay;
  bool nodes_seen = false;
  bool clients_seen = false;
  bool duration_seen = false;
  bool interval_seen = false;
  bool seed_seen = false;
  bool no_chaos_seen = false;
  for (int index = 1; index < argc; ++index) {
    const std::string_view argument(argv[index]);
    if (argument == "--help") {
      usage(std::cout);
      std::exit(0);
    }
    if (!argument.starts_with("--")) {
      throw std::invalid_argument("unexpected positional argument");
    }
    const auto name = option_name(argument);
    if (!seen.insert(name).second) {
      throw std::invalid_argument("duplicate option: " + name);
    }
    if (name == "--keep-success") {
      if (inline_value(argument).has_value()) {
        throw std::invalid_argument("--keep-success takes no value");
      }
      parsed.keep_success = true;
      parsed.options.keep_success = true;
      continue;
    }
    if (name == "--no-chaos") {
      if (inline_value(argument).has_value()) {
        throw std::invalid_argument("--no-chaos takes no value");
      }
      parsed.options.enable_chaos = false;
      no_chaos_seen = true;
      continue;
    }
    auto value = inline_value(argument);
    if (!value.has_value()) {
      if (index + 1 >= argc) {
        throw std::invalid_argument("missing value for " + name);
      }
      value = argv[++index];
    }
    if (value->empty()) {
      throw std::invalid_argument("empty value for " + name);
    }
    if (name == "--nodes") {
      const auto count = number(*value, "node count");
      if (count != 3U && count != 5U) {
        throw std::invalid_argument("nodes must be 3 or 5");
      }
      parsed.options.node_count = static_cast<std::size_t>(count);
      nodes_seen = true;
    } else if (name == "--clients") {
      const auto count = number(*value, "client count");
      if (count == 0U || count > 256U) {
        throw std::invalid_argument("clients must be in [1, 256]");
      }
      parsed.options.client_count = static_cast<std::size_t>(count);
      clients_seen = true;
    } else if (name == "--duration") {
      const auto seconds = number(*value, "duration");
      if (seconds == 0U || seconds > 3600U) {
        throw std::invalid_argument("duration must be in [1, 3600]");
      }
      parsed.options.duration = std::chrono::seconds(seconds);
      duration_seen = true;
    } else if (name == "--action-interval-ms") {
      const auto milliseconds = number(*value, "action interval");
      if (milliseconds < 50U || milliseconds > 60'000U) {
        throw std::invalid_argument("action interval must be in [50, 60000]");
      }
      parsed.options.action_interval = std::chrono::milliseconds(milliseconds);
      interval_seen = true;
    } else if (name == "--seed") {
      parsed.options.seed = number(*value, "seed");
      seed_seen = true;
    } else if (name == "--server") {
      parsed.options.server_path = *value;
    } else if (name == "--artifacts") {
      parsed.options.artifact_directory = *value;
    } else if (name == "--replay") {
      replay = *value;
    } else {
      throw std::invalid_argument("unknown option: " + name);
    }
  }
  if (parsed.options.server_path.empty()) {
    throw std::invalid_argument("--server is required");
  }
  if (replay.has_value() && seed_seen) {
    throw std::invalid_argument("--seed and --replay are incompatible");
  }
  if (replay.has_value() && no_chaos_seen) {
    throw std::invalid_argument("--no-chaos and --replay are incompatible");
  }
  if (replay.has_value()) {
    const auto metadata = forgekv::chaos::read_campaign_config(
        replay->parent_path() / "config.json");
    if (!metadata.ok()) {
      throw std::invalid_argument("replay metadata: " + metadata.error);
    }
    const auto& config = *metadata.config;
    if ((nodes_seen && parsed.options.node_count != config.nodes) ||
        (clients_seen && parsed.options.client_count != config.clients) ||
        (duration_seen && static_cast<std::uint64_t>(
                              parsed.options.duration.count()) !=
                              config.duration_ms) ||
        (interval_seen && static_cast<std::uint64_t>(
                              parsed.options.action_interval.count()) !=
                              config.action_interval_ms)) {
      throw std::invalid_argument("replay options do not match campaign config");
    }
    parsed.options.node_count = config.nodes;
    parsed.options.client_count = config.clients;
    parsed.options.duration = std::chrono::milliseconds(config.duration_ms);
    parsed.options.action_interval =
        std::chrono::milliseconds(config.action_interval_ms);
    parsed.options.seed = config.seed;
    auto timeline = forgekv::chaos::read_timeline(*replay);
    if (!timeline.ok()) {
      throw std::invalid_argument("replay: " + timeline.error);
    }
    parsed.options.script = std::move(timeline.actions);
  }
  if (parsed.options.artifact_directory.empty()) {
    const auto suffix =
        std::chrono::steady_clock::now().time_since_epoch().count();
    parsed.options.artifact_directory =
        std::filesystem::current_path() /
        ("chaos-artifacts-" + std::to_string(suffix));
  }
  parsed.options.overall_timeout =
      parsed.options.duration + std::chrono::seconds(90);
  return parsed;
}

}  // namespace

int main(const int argc, char** argv) {
  try {
    auto parsed = parse(argc, argv);
    std::signal(SIGINT, request_shutdown);
    std::signal(SIGTERM, request_shutdown);
    parsed.options.interrupted = [] { return shutdown_requested != 0; };
    std::cout << "seed=" << parsed.options.seed
              << " artifacts=" << parsed.options.artifact_directory.string()
              << std::endl;
    const auto result =
        forgekv::chaos::ChaosHarness(std::move(parsed.options)).run();
    std::cout << "result=" << (result.ok() ? "pass" : "fail")
              << " converged=" << (result.summary.converged ? "true" : "false")
              << " restart_verified="
              << (result.summary.restart_verified ? "true" : "false")
              << " attempts=" << result.summary.attempts
              << " acknowledged_writes="
              << result.summary.acknowledged_writes;
    if (!result.ok()) {
      std::cout << " diagnostic=" << result.diagnostic;
    }
    std::cout << std::endl;
    if (result.ok() && !parsed.keep_success) {
      std::error_code error;
      constexpr std::array<std::string_view, 7> bulky_artifacts{
          "data", "logs", "metrics", "history.jsonl", "timeline.jsonl",
          "replay.txt", "children.txt"};
      for (const auto name : bulky_artifacts) {
        std::filesystem::remove_all(result.artifact_directory / name, error);
        if (error) {
          std::cerr << "cleanup failed: " << error.message() << '\n';
          return 1;
        }
      }
    }
    return result.ok() ? 0 : 1;
  } catch (const std::exception& error) {
    std::cerr << "forgekv-chaos: " << error.what() << '\n';
    usage(std::cerr);
    return 2;
  }
}
