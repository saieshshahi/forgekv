#pragma once

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace forgekv::benchmarking {

class LatencyHistogram final {
 public:
  static constexpr std::chrono::microseconds kMaximum{60'000'000};

  void observe(std::chrono::microseconds latency) noexcept;
  void merge(const LatencyHistogram& other) noexcept;

  [[nodiscard]] std::optional<std::chrono::microseconds> quantile(
      double fraction) const noexcept;
  [[nodiscard]] std::uint64_t count() const noexcept { return count_; }
  [[nodiscard]] std::uint64_t underflow_count() const noexcept {
    return underflow_count_;
  }
  [[nodiscard]] std::uint64_t overflow_count() const noexcept {
    return overflow_count_;
  }
  [[nodiscard]] std::chrono::microseconds maximum() const noexcept {
    return maximum_;
  }

 private:
  static constexpr std::size_t kSubBuckets = 128U;
  static constexpr std::size_t kExponents = 26U;
  static constexpr std::size_t kBucketCount = kSubBuckets * kExponents;

  std::array<std::uint64_t, kBucketCount> buckets_{};
  std::uint64_t count_{};
  std::uint64_t underflow_count_{};
  std::uint64_t overflow_count_{};
  std::chrono::microseconds maximum_{};
};

}  // namespace forgekv::benchmarking
