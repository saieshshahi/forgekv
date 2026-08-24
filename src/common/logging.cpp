#include "common/logging.h"

#include <atomic>
#include <iostream>
#include <mutex>
#include <utility>

namespace forgekv::common {
namespace {

std::atomic<Severity> runtime_minimum_level{Severity::info};
std::mutex sink_mutex;

void default_sink(const Severity severity, const std::string_view message) {
  static std::mutex output_mutex;
  const std::lock_guard lock(output_mutex);
  std::cerr << '[' << severity_name(severity) << "] " << message << '\n';
}

Logger::Sink sink = default_sink;

}  // namespace

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

void Logger::reset_sink() {
  set_sink(default_sink);
}

void Logger::write(const Severity severity, const std::string_view message) {
  if (!enabled(severity)) {
    return;
  }

  Sink current_sink;
  {
    const std::lock_guard lock(sink_mutex);
    current_sink = sink;
  }
  current_sink(severity, message);
}

}  // namespace forgekv::common
