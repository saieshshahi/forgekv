#pragma once

#include "chaos/types.h"

#include <cstdint>

namespace forgekv::chaos {

class ChaosScheduler final {
 public:
  ChaosScheduler(std::uint64_t seed, SchedulerLimits limits);

  [[nodiscard]] ChaosAction next(std::uint64_t planned_offset_us,
                                 const ClusterView& view);
  [[nodiscard]] ChaosAction normalize(ChaosAction action,
                                      const ClusterView& view) const;

 private:
  [[nodiscard]] std::uint64_t random();
  [[nodiscard]] std::uint64_t choose(std::uint64_t count);

  std::uint64_t state_;
  SchedulerLimits limits_;
};

}  // namespace forgekv::chaos
