#pragma once

#include <chrono>
#include <cstddef>
#include <functional>
#include <string>
#include <vector>

namespace forgekv::chaos {

struct CommandOptions final {
  std::chrono::milliseconds timeout{std::chrono::seconds(30)};
  std::size_t maximum_output_bytes{64U * 1024U};
  std::function<bool()> interrupted;
};

struct CommandResult final {
  int exit_code{-1};
  bool timed_out{};
  bool interrupted{};
  std::string output;
  std::string error;
  [[nodiscard]] bool ok() const noexcept {
    return exit_code == 0 && !timed_out && !interrupted && error.empty();
  }
};

class CommandExecutor {
 public:
  virtual ~CommandExecutor() = default;
  virtual CommandResult run(const std::vector<std::string>& arguments,
                            const CommandOptions& options) = 0;
};

class PosixCommandExecutor final : public CommandExecutor {
 public:
  CommandResult run(const std::vector<std::string>& arguments,
                    const CommandOptions& options) override;
};

}  // namespace forgekv::chaos
