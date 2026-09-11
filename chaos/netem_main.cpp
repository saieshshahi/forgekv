#include "chaos/netem.h"
#include "chaos/netem_runner.h"
#include "chaos/process_runner.h"

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cctype>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <initializer_list>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <sys/stat.h>
#include <system_error>
#include <thread>
#include <unistd.h>
#include <vector>

namespace {

using forgekv::chaos::NetemProfile;
using forgekv::chaos::NetemRunnerOptions;

volatile std::sig_atomic_t interrupted = 0;

void request_stop(const int) { interrupted = 1; }

void usage(std::ostream& output) {
  output <<
      "usage: forgekv-netem --chaos PATH --server PATH --artifacts PATH "
      "[options]\n"
      "  --nodes 3|5              cluster size (default: 3)\n"
      "  --clients 1..256         clients (default: 4)\n"
      "  --duration 1..3600       seconds per profile (default: 10)\n"
      "  --seed UINT64            workload seed\n"
      "  --profile NAME           repeatable known profile; default matrix\n"
      "Profiles: baseline, latency-10ms, latency-50ms, latency-100ms,\n"
      "          loss-0.1pct, loss-1pct, loss-5pct, jitter-smoke,\n"
      "          reorder-smoke\n"
      "Run as root. Every qdisc is confined to lo in a disposable namespace.\n";
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
  if (separator == std::string_view::npos) return std::nullopt;
  return std::string(argument.substr(separator + 1U));
}

struct Parsed final {
  NetemRunnerOptions options;
};

Parsed parse(const int argc, char** argv) {
  Parsed parsed;
  std::set<std::string> seen;
  std::set<std::string> profile_names;
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
    if (name != "--profile" && !seen.insert(name).second) {
      throw std::invalid_argument("duplicate option: " + name);
    }
    auto value = inline_value(argument);
    if (!value.has_value()) {
      if (index + 1 >= argc) {
        throw std::invalid_argument("missing value for " + name);
      }
      value = argv[++index];
    }
    if (value->empty()) throw std::invalid_argument("empty value for " + name);

    if (name == "--chaos") {
      parsed.options.chaos_path = *value;
    } else if (name == "--server") {
      parsed.options.server_path = *value;
    } else if (name == "--artifacts") {
      parsed.options.output_directory = *value;
    } else if (name == "--nodes") {
      const auto value_number = number(*value, "node count");
      if (value_number != 3U && value_number != 5U) {
        throw std::invalid_argument("nodes must be 3 or 5");
      }
      parsed.options.nodes = static_cast<std::size_t>(value_number);
    } else if (name == "--clients") {
      const auto value_number = number(*value, "client count");
      if (value_number == 0U || value_number > 256U) {
        throw std::invalid_argument("clients must be in [1, 256]");
      }
      parsed.options.clients = static_cast<std::size_t>(value_number);
    } else if (name == "--duration") {
      const auto value_number = number(*value, "duration");
      if (value_number == 0U || value_number > 3600U) {
        throw std::invalid_argument("duration must be in [1, 3600]");
      }
      parsed.options.duration = std::chrono::seconds(value_number);
    } else if (name == "--seed") {
      parsed.options.seed = number(*value, "seed");
    } else if (name == "--profile") {
      const auto profile = forgekv::chaos::named_netem_profile(*value);
      if (!profile.has_value()) {
        throw std::invalid_argument("unknown netem profile: " + *value);
      }
      if (!profile_names.insert(*value).second) {
        throw std::invalid_argument("duplicate netem profile: " + *value);
      }
      parsed.options.profiles.push_back(*profile);
    } else {
      throw std::invalid_argument("unknown option: " + name);
    }
  }
  if (parsed.options.chaos_path.empty() || parsed.options.server_path.empty() ||
      parsed.options.output_directory.empty()) {
    throw std::invalid_argument("--chaos, --server, and --artifacts are required");
  }
  if (parsed.options.profiles.empty()) {
    parsed.options.profiles = forgekv::chaos::required_netem_profiles();
  }
  return parsed;
}

std::string json_string(const std::string_view input) {
  std::string result{"\""};
  for (const unsigned char character : input) {
    switch (character) {
      case '\\': result += "\\\\"; break;
      case '"': result += "\\\""; break;
      case '\n': result += "\\n"; break;
      case '\r': result += "\\r"; break;
      case '\t': result += "\\t"; break;
      default:
        if (character < 0x20U) {
          result += "?";
        } else {
          result.push_back(static_cast<char>(character));
        }
    }
  }
  result.push_back('"');
  return result;
}

void write_atomic(const std::filesystem::path& path, const std::string& text) {
  const auto temporary = path.string() + ".tmp";
  {
    std::ofstream output(temporary, std::ios::binary | std::ios::trunc);
    output.write(text.data(), static_cast<std::streamsize>(text.size()));
    output.flush();
    if (!output) throw std::runtime_error("write evidence: " + path.string());
  }
  std::error_code error;
  std::filesystem::rename(temporary, path, error);
  if (error) {
    std::filesystem::remove(temporary);
    throw std::runtime_error("publish evidence: " + error.message());
  }
}

std::string cpu_description() {
  std::ifstream input("/proc/cpuinfo");
  std::string line;
  std::size_t bytes = 0U;
  while (std::getline(input, line) && bytes < 64U * 1024U) {
    bytes += line.size() + 1U;
    if (line.starts_with("model name")) {
      const auto separator = line.find(':');
      if (separator != std::string::npos) {
        return "cpu_model=" + line.substr(separator + 1U) + "\n";
      }
    }
  }
  return "cpu_model=unavailable\n";
}

void create_private_artifacts_path(const std::filesystem::path& path) {
  std::filesystem::path current;
  for (const auto& component : path) {
    current /= component;
    struct stat metadata {};
    if (::lstat(current.c_str(), &metadata) != 0) {
      if (errno != ENOENT || ::mkdir(current.c_str(), 0700) != 0 ||
          ::lstat(current.c_str(), &metadata) != 0) {
        throw std::invalid_argument("cannot create private artifacts path");
      }
    } else if (current == path) {
      throw std::invalid_argument("artifacts path must be absent");
    }
    if (S_ISLNK(metadata.st_mode)) {
      throw std::invalid_argument(
          "artifacts path must not contain a symlink component");
    }
    if (!S_ISDIR(metadata.st_mode) || metadata.st_uid != 0U) {
      throw std::invalid_argument(
          "artifacts path requires a trusted root-owned parent");
    }
    const auto writable = metadata.st_mode & (S_IWGRP | S_IWOTH);
    const auto sticky = (metadata.st_mode & S_ISVTX) != 0;
    if (writable != 0 && !sticky) {
      throw std::invalid_argument(
          "artifacts path requires a trusted root-owned parent");
    }
  }
}

std::string executable_sha256(forgekv::chaos::CommandExecutor& executor,
                              const std::filesystem::path& sha256_path,
                              const std::filesystem::path& path,
                              const std::string_view label) {
  const auto result =
      executor.run({sha256_path.string(), "--", path.string()}, {});
  const auto separator = result.output.find_first_of(" \t\r\n");
  const auto hash = result.output.substr(0U, separator);
  const auto hexadecimal = hash.size() == 64U &&
                           std::ranges::all_of(hash, [](const char character) {
                             return std::isxdigit(
                                        static_cast<unsigned char>(character)) !=
                                    0;
                           });
  if (!result.ok() || !hexadecimal) {
    throw std::runtime_error("cannot identify " + std::string(label) +
                             " executable build");
  }
  return std::string(label) + "_sha256=" + hash + "\n";
}

std::filesystem::path resolve_tool(
    const std::initializer_list<std::filesystem::path> candidates,
    const std::string_view name) {
  for (const auto& candidate : candidates) {
    std::error_code error;
    const auto resolved = std::filesystem::canonical(candidate, error);
    if (!error && std::filesystem::is_regular_file(resolved) &&
        ::access(resolved.c_str(), X_OK) == 0) {
      return resolved;
    }
  }
  throw std::runtime_error("required " + std::string(name) +
                           " tool is unavailable");
}

std::filesystem::path validate_paths(NetemRunnerOptions& options) {
  std::error_code error;
  options.chaos_path = std::filesystem::canonical(options.chaos_path, error);
  if (error || !std::filesystem::is_regular_file(options.chaos_path) ||
      ::access(options.chaos_path.c_str(), X_OK) != 0) {
    throw std::invalid_argument("chaos path is not an executable file");
  }
  options.server_path = std::filesystem::canonical(options.server_path, error);
  if (error || !std::filesystem::is_regular_file(options.server_path) ||
      ::access(options.server_path.c_str(), X_OK) != 0) {
    throw std::invalid_argument("server path is not an executable file");
  }
  auto output = std::filesystem::absolute(options.output_directory, error)
                    .lexically_normal();
  if (error || output.empty() || output == output.root_path() ||
      output == std::filesystem::current_path()) {
    throw std::invalid_argument("unsafe artifacts path");
  }
  if (const auto* home = std::getenv("HOME"); home != nullptr &&
      output == std::filesystem::path(home).lexically_normal()) {
    throw std::invalid_argument("artifacts path must not be a home directory");
  }
  create_private_artifacts_path(output);
  options.output_directory = output;
  return output;
}

std::string result_json(const forgekv::chaos::NetemProfileResult& result,
                        const NetemRunnerOptions& options) {
  const auto configured_ms = options.duration.count() * 1000;
  return "{\"version\":1,\"profile\":" + json_string(result.profile.name) +
         ",\"fault_kind\":" +
         json_string(forgekv::chaos::netem_fault_label(result.profile)) +
         ",\"delay_us\":" + std::to_string(result.profile.delay_us) +
         ",\"jitter_us\":" + std::to_string(result.profile.jitter_us) +
         ",\"loss_basis_points\":" +
         std::to_string(result.profile.loss_basis_points) +
         ",\"reorder_basis_points\":" +
         std::to_string(result.profile.reorder_basis_points) +
         ",\"seed\":" + std::to_string(options.seed) +
         ",\"nodes\":" + std::to_string(options.nodes) +
         ",\"clients\":" + std::to_string(options.clients) +
         ",\"configured_duration_ms\":" + std::to_string(configured_ms) +
         ",\"wall_duration_ms\":" +
         std::to_string(result.wall_duration.count()) +
         ",\"exit_code\":" + std::to_string(result.workload_exit_code) +
         ",\"passed\":" + (result.ok() ? "true" : "false") +
         ",\"attempts\":" + std::to_string(result.summary.attempts) +
         ",\"acknowledged_writes\":" +
         std::to_string(result.summary.acknowledged_writes) +
         ",\"actions\":" + std::to_string(result.summary.actions) +
         ",\"converged\":" + (result.summary.converged ? "true" : "false") +
         ",\"restart_verified\":" +
         (result.summary.restart_verified ? "true" : "false") +
         ",\"qdisc_packets\":" + std::to_string(result.qdisc.packets) +
         ",\"qdisc_dropped\":" + std::to_string(result.qdisc.dropped) +
         ",\"diagnostic\":" + json_string(result.error) + "}\n";
}

}  // namespace

int main(const int argc, char** argv) {
  try {
    auto parsed = parse(argc, argv);
    if (::geteuid() != 0) {
      throw std::invalid_argument(
          "root is required; use a disposable WSL/Linux root invocation");
    }
    const auto output = validate_paths(parsed.options);
    std::signal(SIGINT, request_stop);
    std::signal(SIGTERM, request_stop);
    parsed.options.interrupted = [] { return interrupted != 0; };

    forgekv::chaos::PosixCommandExecutor executor;
    const auto uname_path =
        resolve_tool({"/usr/bin/uname", "/bin/uname"}, "uname");
    const auto ip_path = resolve_tool({"/usr/sbin/ip", "/sbin/ip"}, "ip");
    const auto tc_path = resolve_tool({"/usr/sbin/tc", "/sbin/tc"}, "tc");
    const auto sha256_path = resolve_tool(
        {"/usr/bin/sha256sum", "/bin/sha256sum"}, "sha256sum");
    parsed.options.ip_path = ip_path;
    parsed.options.tc_path = tc_path;
    const auto uname = executor.run({uname_path.string(), "-sr"}, {});
    const auto tc = executor.run({tc_path.string(), "-V"}, {});
    const auto ip = executor.run({ip_path.string(), "-V"}, {});
    if (!uname.ok() || !tc.ok() || !ip.ok()) {
      throw std::runtime_error(
          "required uname/ip/tc tools are unavailable");
    }
    std::string invocation{"invocation_argv=["};
    for (int index = 0; index < argc; ++index) {
      if (index != 0) invocation += ',';
      invocation += json_string(argv[index]);
    }
    invocation += "]\n";
    std::error_code self_error;
    const auto self_path =
        std::filesystem::canonical("/proc/self/exe", self_error);
    if (self_error || self_path.empty()) {
      throw std::runtime_error("cannot resolve netem executable build");
    }
    const auto netem_identity =
        executable_sha256(executor, sha256_path, self_path, "netem");
    const auto chaos_identity = executable_sha256(
        executor, sha256_path, parsed.options.chaos_path, "chaos");
    const auto server_identity = executable_sha256(
        executor, sha256_path, parsed.options.server_path, "server");
    const auto environment =
        uname.output + tc.output + ip.output + cpu_description() +
        "compiler=" + std::string(__VERSION__) + "\n" + netem_identity +
        chaos_identity + server_identity +
        "logical_cpus=" + std::to_string(std::thread::hardware_concurrency()) +
        "\nchaos_path=" + parsed.options.chaos_path.string() +
        "\nserver_path=" + parsed.options.server_path.string() + "\n" +
        invocation +
        "fault_semantics=kernel packet impairment; not proxy reset; not partition\n";
    write_atomic(output / "environment.txt", environment);

    forgekv::chaos::NetemRunner runner(
        executor, static_cast<std::uint32_t>(::geteuid()),
        static_cast<std::uint64_t>(::getpid()));
    auto matrix = runner.run(parsed.options);
    std::string identity_error;
    try {
      if (netem_identity !=
              executable_sha256(executor, sha256_path, self_path, "netem") ||
          chaos_identity != executable_sha256(
                                executor, sha256_path,
                                parsed.options.chaos_path, "chaos") ||
          server_identity != executable_sha256(
                                 executor, sha256_path,
                                 parsed.options.server_path, "server")) {
        identity_error = "executable identity changed during experiment";
      }
    } catch (const std::exception& error) {
      identity_error =
          "executable identity changed or became unreadable during experiment: " +
          std::string(error.what());
    }
    if (!identity_error.empty()) {
      if (!matrix.error.empty()) matrix.error += "; ";
      matrix.error += identity_error;
      for (auto& profile : matrix.profiles) {
        if (!profile.error.empty()) profile.error += "; ";
        profile.error += identity_error;
      }
    }
    std::string jsonl;
    std::string markdown =
        "# ForgeKV netem matrix\n\n"
        "| Profile | Attempts/s | Ack mutations/s | Drops | Converged | Restart | Valid |\n"
        "| --- | ---: | ---: | ---: | --- | --- | --- |\n";
    for (const auto& result : matrix.profiles) {
      jsonl += result_json(result, parsed.options);
      const auto seconds = static_cast<std::uint64_t>(parsed.options.duration.count());
      markdown += "| " + result.profile.name + " | " +
                  std::to_string(result.summary.attempts / seconds) + " | " +
                  std::to_string(result.summary.acknowledged_writes / seconds) +
                  " | " + std::to_string(result.qdisc.dropped) + " | " +
                  (result.summary.converged ? "yes" : "no") + " | " +
                  (result.summary.restart_verified ? "yes" : "no") + " | " +
                  (result.ok() ? "yes" : "no") + " |\n";
      const auto profile_directory = output / result.profile.name;
      std::error_code directory_error;
      std::filesystem::create_directories(profile_directory, directory_error);
      if (directory_error) {
        throw std::runtime_error("create profile evidence: " +
                                 directory_error.message());
      }
      write_atomic(profile_directory / "netem-runner.log",
                   result.workload_output);
      write_atomic(profile_directory / "qdisc.txt", result.qdisc_output);
    }
    write_atomic(output / "results.jsonl", jsonl);
    write_atomic(output / "summary.md", markdown);
    if (!matrix.ok()) {
      std::cerr << "forgekv-netem: " << matrix.error << '\n';
      return 1;
    }
    std::cout << "result=pass profiles=" << matrix.profiles.size()
              << " artifacts=" << output.string() << '\n';
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "forgekv-netem: " << error.what() << '\n';
    usage(std::cerr);
    return 2;
  }
}
