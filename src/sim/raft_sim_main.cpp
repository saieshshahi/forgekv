#include "sim/raft_simulator.h"

#include <charconv>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string_view>

namespace {

std::uint64_t parse_number(const std::string_view text,
                           const std::string_view option) {
  std::uint64_t value{};
  const auto [end, error] =
      std::from_chars(text.data(), text.data() + text.size(), value);
  if (error != std::errc{} || end != text.data() + text.size()) {
    throw std::invalid_argument("invalid value for " + std::string(option));
  }
  return value;
}

void usage(std::ostream& output) {
  output << "usage: forgekv_raft_sim [--seed N] [--steps N]\n";
}

}  // namespace

int main(const int argc, char** argv) {
  std::uint64_t seed = 1;
  std::uint64_t steps = 100'000;
  try {
    for (int index = 1; index < argc; ++index) {
      const std::string_view option(argv[index]);
      if (option == "--help") {
        usage(std::cout);
        return 0;
      }
      if ((option != "--seed" && option != "--steps") || index + 1 >= argc) {
        throw std::invalid_argument("unknown or incomplete option");
      }
      const auto value = parse_number(argv[++index], option);
      (option == "--seed" ? seed : steps) = value;
    }
    if (steps > std::numeric_limits<std::size_t>::max()) {
      throw std::invalid_argument("--steps exceeds this platform's limit");
    }

    forgekv::sim::RaftSimulator simulator({
        .voters = {1, 2, 3},
        .election_timeout_min = 100,
        .election_timeout_max = 200,
        .heartbeat_interval = 25,
        .seed = seed,
        .max_pending_messages = 10'000,
        .max_trace_records = 2'000,
    });
    std::cout << "ForgeKV Raft simulation seed=" << seed
              << " steps=" << steps << '\n';
    simulator.run_random(static_cast<std::size_t>(steps));
    simulator.check_invariants();
    std::cout << "PASS operations=" << simulator.operation_count()
              << " logical_time=" << simulator.now() << '\n';
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "FAIL seed=" << seed << " steps=" << steps << '\n'
              << error.what() << '\n';
    return 1;
  }
}
