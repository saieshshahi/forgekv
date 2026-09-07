#include "chaos/process_runner.h"

#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstring>
#include <fcntl.h>
#include <poll.h>
#include <string>
#include <sys/types.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace forgekv::chaos {
namespace {

void terminate_group(const pid_t child) noexcept {
  if (child <= 0) return;
  static_cast<void>(::kill(-child, SIGTERM));
  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(250);
  int status = 0;
  while (std::chrono::steady_clock::now() < deadline) {
    const auto result = ::waitpid(child, &status, WNOHANG);
    if (result == child || (result < 0 && errno == ECHILD)) return;
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  static_cast<void>(::kill(-child, SIGKILL));
  while (::waitpid(child, &status, 0) < 0 && errno == EINTR) {
  }
}

void drain_pipe(const int descriptor, const std::size_t maximum,
                CommandResult& result, bool& exceeded) {
  char buffer[4096];
  while (true) {
    const auto bytes = ::read(descriptor, buffer, sizeof(buffer));
    if (bytes > 0) {
      const auto count = static_cast<std::size_t>(bytes);
      const auto remaining = result.output.size() < maximum
                                 ? maximum - result.output.size()
                                 : 0U;
      if (result.output.size() < maximum) {
        result.output.append(buffer, std::min(count, remaining));
      }
      exceeded = exceeded || count > remaining;
      continue;
    }
    if (bytes < 0 && errno == EINTR) continue;
    if (bytes < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return;
    return;
  }
}

}  // namespace

CommandResult PosixCommandExecutor::run(
    const std::vector<std::string>& arguments, const CommandOptions& options) {
  CommandResult result;
  if (arguments.empty() || arguments.front().empty() ||
      options.timeout.count() <= 0 || options.maximum_output_bytes == 0U) {
    result.error = "invalid command options";
    return result;
  }

  int descriptors[2]{-1, -1};
  if (::pipe2(descriptors, O_CLOEXEC | O_NONBLOCK) != 0) {
    result.error = "pipe2: " + std::string(std::strerror(errno));
    return result;
  }
  const auto child = ::fork();
  if (child < 0) {
    result.error = "fork: " + std::string(std::strerror(errno));
    static_cast<void>(::close(descriptors[0]));
    static_cast<void>(::close(descriptors[1]));
    return result;
  }
  if (child == 0) {
    static_cast<void>(::setpgid(0, 0));
    static_cast<void>(::dup2(descriptors[1], STDOUT_FILENO));
    static_cast<void>(::dup2(descriptors[1], STDERR_FILENO));
    static_cast<void>(::close(descriptors[0]));
    static_cast<void>(::close(descriptors[1]));
    std::vector<char*> argv;
    argv.reserve(arguments.size() + 1U);
    for (const auto& argument : arguments) {
      argv.push_back(const_cast<char*>(argument.c_str()));
    }
    argv.push_back(nullptr);
    ::execvp(argv.front(), argv.data());
    ::_exit(126);
  }

  static_cast<void>(::close(descriptors[1]));
  static_cast<void>(::setpgid(child, child));
  const auto deadline = std::chrono::steady_clock::now() + options.timeout;
  bool exceeded = false;
  int status = 0;
  while (true) {
    drain_pipe(descriptors[0], options.maximum_output_bytes, result, exceeded);
    const auto waited = ::waitpid(child, &status, WNOHANG);
    if (waited == child) break;
    if (waited < 0 && errno != EINTR) {
      result.error = "waitpid: " + std::string(std::strerror(errno));
      terminate_group(child);
      break;
    }
    if (exceeded) {
      result.error = "command output exceeds configured limit";
      terminate_group(child);
      break;
    }
    if (options.interrupted && options.interrupted()) {
      result.interrupted = true;
      result.error = "command interrupted";
      terminate_group(child);
      break;
    }
    if (std::chrono::steady_clock::now() >= deadline) {
      result.timed_out = true;
      result.error = "command timed out";
      terminate_group(child);
      break;
    }
    pollfd descriptor{.fd = descriptors[0], .events = POLLIN, .revents = 0};
    static_cast<void>(::poll(&descriptor, 1, 10));
  }
  drain_pipe(descriptors[0], options.maximum_output_bytes, result, exceeded);
  static_cast<void>(::close(descriptors[0]));
  if (result.error.empty() && exceeded) {
    result.error = "command output exceeds configured limit";
  }
  if (WIFEXITED(status)) {
    result.exit_code = WEXITSTATUS(status);
  } else if (WIFSIGNALED(status)) {
    result.exit_code = 128 + WTERMSIG(status);
  }
  return result;
}

}  // namespace forgekv::chaos
