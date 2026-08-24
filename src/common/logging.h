#pragma once

#include <cstdint>
#include <functional>
#include <string_view>

namespace forgekv::common {

enum class Severity : std::uint8_t {
  trace = 0,
  debug = 1,
  info = 2,
  warning = 3,
  error = 4,
  critical = 5,
  off = 6,
};

[[nodiscard]] constexpr std::string_view severity_name(const Severity severity) noexcept {
  switch (severity) {
    case Severity::trace:
      return "TRACE";
    case Severity::debug:
      return "DEBUG";
    case Severity::info:
      return "INFO";
    case Severity::warning:
      return "WARN";
    case Severity::error:
      return "ERROR";
    case Severity::critical:
      return "CRITICAL";
    case Severity::off:
      return "OFF";
  }
  return "UNKNOWN";
}

class Logger final {
 public:
  using Sink = std::function<void(Severity, std::string_view)>;

  Logger() = delete;

  static void set_runtime_minimum(Severity severity) noexcept;
  [[nodiscard]] static Severity runtime_minimum() noexcept;
  [[nodiscard]] static bool enabled(Severity severity) noexcept;

  static void set_sink(Sink sink);
  static void reset_sink();
  static void write(Severity severity, std::string_view message);
};

}  // namespace forgekv::common

#ifndef FORGEKV_COMPILED_LOG_LEVEL
#define FORGEKV_COMPILED_LOG_LEVEL 0
#endif

#if FORGEKV_COMPILED_LOG_LEVEL <= 0
#define FORGEKV_LOG(severity, message)                                      \
  do {                                                                      \
    constexpr auto forgekv_log_severity = (severity);                       \
    if (::forgekv::common::Logger::enabled(forgekv_log_severity)) {         \
      ::forgekv::common::Logger::write(forgekv_log_severity, (message));    \
    }                                                                       \
  } while (false)
#else
#define FORGEKV_LOG(severity, message)                                      \
  do {                                                                      \
    constexpr auto forgekv_log_severity = (severity);                       \
    if constexpr (static_cast<int>(forgekv_log_severity) >=                 \
                  FORGEKV_COMPILED_LOG_LEVEL) {                             \
      if (::forgekv::common::Logger::enabled(forgekv_log_severity)) {       \
        ::forgekv::common::Logger::write(forgekv_log_severity, (message));  \
      }                                                                     \
    }                                                                       \
  } while (false)
#endif
