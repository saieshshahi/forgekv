#include "common/logging.h"

#include <atomic>
#include <array>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>

namespace forgekv::common {
namespace {

std::atomic<Severity> runtime_minimum_level{Severity::info};
std::mutex sink_mutex;

std::string json_escape(const std::string_view value) {
  std::ostringstream output;
  output << std::hex << std::setfill('0');
  for (const unsigned char byte : value) {
    switch (byte) {
      case '"':
        output << "\\\"";
        break;
      case '\\':
        output << "\\\\";
        break;
      case '\b':
        output << "\\b";
        break;
      case '\f':
        output << "\\f";
        break;
      case '\n':
        output << "\\n";
        break;
      case '\r':
        output << "\\r";
        break;
      case '\t':
        output << "\\t";
        break;
      default:
        if (byte < 0x20U) {
          output << "\\u00" << std::setw(2) << static_cast<unsigned>(byte);
        } else {
          output << static_cast<char>(byte);
        }
    }
  }
  return output.str();
}

void default_sink(const LogRecord& record) {
  static std::mutex output_mutex;
  const std::lock_guard lock(output_mutex);
  std::cerr << format_log_record(record, std::chrono::system_clock::now())
            << '\n';
}

Logger::Sink sink = default_sink;

}  // namespace

std::string format_log_record(
    const LogRecord& record,
    const std::chrono::system_clock::time_point timestamp) {
  const auto epoch = timestamp.time_since_epoch();
  const auto seconds =
      std::chrono::duration_cast<std::chrono::seconds>(epoch);
  const auto micros =
      std::chrono::duration_cast<std::chrono::microseconds>(epoch - seconds);
  const std::time_t wall_seconds =
      std::chrono::system_clock::to_time_t(
          std::chrono::system_clock::time_point(seconds));
  std::tm utc{};
  static_cast<void>(::gmtime_r(&wall_seconds, &utc));

  std::ostringstream output;
  output << "{\"timestamp\":\"" << std::put_time(&utc, "%Y-%m-%dT%H:%M:%S")
         << '.' << std::setfill('0') << std::setw(6) << micros.count()
         << "Z\",\"severity\":\"" << severity_name(record.severity)
         << "\",\"message\":\"" << json_escape(record.message) << '"';
  if (record.request_id.has_value()) {
    output << ",\"request_id\":" << *record.request_id;
  }
  output << '}';
  return output.str();
}

void Logger::set_runtime_minimum(const Severity severity) noexcept {
  runtime_minimum_level.store(severity, std::memory_order_relaxed);
}

Severity Logger::runtime_minimum() noexcept {
  return runtime_minimum_level.load(std::memory_order_relaxed);
}

bool Logger::enabled(const Severity severity) noexcept {
  const auto minimum = runtime_minimum_level.load(std::memory_order_relaxed);
  return severity != Severity::off && severity >= minimum;
}

void Logger::set_sink(Sink new_sink) {
  const std::lock_guard lock(sink_mutex);
  sink = new_sink ? std::move(new_sink) : Sink{default_sink};
}

void Logger::set_sink(LegacySink new_sink) {
  if (!new_sink) {
    reset_sink();
    return;
  }
  set_sink(Sink{[legacy = std::move(new_sink)](const LogRecord& record) {
    legacy(record.severity, record.message);
  }});
}

void Logger::reset_sink() {
  set_sink(default_sink);
}

void Logger::write(const Severity severity, const std::string_view message,
                   const std::optional<std::uint64_t> request_id) noexcept {
  try {
    if (!enabled(severity)) {
      return;
    }

    Sink current_sink;
    {
      const std::lock_guard lock(sink_mutex);
      current_sink = sink;
    }
    current_sink(LogRecord{.severity = severity,
                           .message = std::string(message),
                           .request_id = request_id});
  } catch (...) {
    // Observability must never change application behavior.
  }
}

}  // namespace forgekv::common
