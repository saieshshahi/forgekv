#include "bench/system/latency_histogram.h"

#include <algorithm>
#include <bit>
#include <cmath>
#include <limits>

namespace forgekv::benchmarking {
namespace {

constexpr std::size_t kSubBuckets = 128U;

std::uint64_t saturated_add(const std::uint64_t left,
                            const std::uint64_t right) noexcept {
  if (right > std::numeric_limits<std::uint64_t>::max() - left) {
    return std::numeric_limits<std::uint64_t>::max();
  }
  return left + right;
}

std::size_t bucket_index(const std::uint64_t value) noexcept {
  const auto exponent = static_cast<std::size_t>(std::bit_width(value) - 1U);
  const std::uint64_t base = std::uint64_t{1} << exponent;
  const auto sub_bucket = static_cast<std::size_t>(
      ((value - base) * kSubBuckets) / base);
  return exponent * kSubBuckets + std::min(sub_bucket, kSubBuckets - 1U);
}

std::uint64_t bucket_upper(const std::size_t index) noexcept {
  const auto exponent = index / kSubBuckets;
  const auto sub_bucket = index % kSubBuckets;
  const std::uint64_t base = std::uint64_t{1} << exponent;
  const auto numerator = (sub_bucket + 1U) * base;
  const auto width_end = (numerator + kSubBuckets - 1U) / kSubBuckets;
  return base + width_end - 1U;
}

}  // namespace

void LatencyHistogram::observe(const std::chrono::microseconds latency) noexcept {
  if (latency.count() <= 0) {
    underflow_count_ = saturated_add(underflow_count_, 1U);
    return;
  }
  if (latency > kMaximum) {
    overflow_count_ = saturated_add(overflow_count_, 1U);
    return;
  }
  const auto value = static_cast<std::uint64_t>(latency.count());
  auto& bucket = buckets_[bucket_index(value)];
  bucket = saturated_add(bucket, 1U);
  count_ = saturated_add(count_, 1U);
  maximum_ = std::max(maximum_, latency);
}

void LatencyHistogram::merge(const LatencyHistogram& other) noexcept {
  for (std::size_t index = 0; index < buckets_.size(); ++index) {
    buckets_[index] = saturated_add(buckets_[index], other.buckets_[index]);
  }
  count_ = saturated_add(count_, other.count_);
  underflow_count_ = saturated_add(underflow_count_, other.underflow_count_);
  overflow_count_ = saturated_add(overflow_count_, other.overflow_count_);
  maximum_ = std::max(maximum_, other.maximum_);
}

std::optional<std::chrono::microseconds> LatencyHistogram::quantile(
    const double fraction) const noexcept {
  if (count_ == 0U || !std::isfinite(fraction) || fraction <= 0.0 ||
      fraction > 1.0) {
    return std::nullopt;
  }
  const auto rank = static_cast<std::uint64_t>(
      std::ceil(fraction * static_cast<double>(count_)));
  std::uint64_t cumulative = 0U;
  for (std::size_t index = 0; index < buckets_.size(); ++index) {
    cumulative = saturated_add(cumulative, buckets_[index]);
    if (cumulative >= rank) {
      const auto upper = std::min<std::uint64_t>(
          bucket_upper(index), static_cast<std::uint64_t>(maximum_.count()));
      return std::chrono::microseconds(upper);
    }
  }
  return std::nullopt;
}

}  // namespace forgekv::benchmarking
