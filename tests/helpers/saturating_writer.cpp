#include <array>
#include <cerrno>
#include <chrono>
#include <fcntl.h>
#include <sys/wait.h>
#include <unistd.h>

int main() {
  const auto flags = ::fcntl(STDOUT_FILENO, F_GETFL);
  if (flags < 0 || ::fcntl(STDOUT_FILENO, F_SETFL, flags & ~O_NONBLOCK) < 0) {
    return 2;
  }
  constexpr int writers = 64;
  for (int index = 0; index < writers; ++index) {
    const auto child = ::fork();
    if (child < 0) return 3;
    if (child == 0) {
      const auto deadline = std::chrono::steady_clock::now() +
                            std::chrono::seconds(2);
      const std::array<char, 4096> bytes{};
      while (std::chrono::steady_clock::now() < deadline) {
        const auto written =
            ::write(STDOUT_FILENO, bytes.data(), bytes.size());
        if (written < 0 && errno != EINTR) return 4;
      }
      return 0;
    }
  }
  for (int index = 0; index < writers; ++index) {
    while (::wait(nullptr) < 0 && errno == EINTR) {
    }
  }
  return 0;
}
