#pragma once

#include "cluster/state_machine.h"

#include <cstddef>
#include <vector>

namespace forgekv::cluster {

[[nodiscard]] std::vector<std::byte> encode_snapshot_state(
    const SnapshotState& state);
[[nodiscard]] DecodeResult<SnapshotState> decode_snapshot_state(
    const std::vector<std::byte>& bytes);

}  // namespace forgekv::cluster
