#pragma once

#include "cluster/codecs.h"

#include <cstddef>
#include <string>
#include <unordered_map>
#include <vector>

namespace forgekv::cluster {

using SnapshotState =
    std::unordered_map<std::string, std::vector<std::byte>>;

[[nodiscard]] std::vector<std::byte> encode_snapshot_state(
    const SnapshotState& state);
[[nodiscard]] DecodeResult<SnapshotState> decode_snapshot_state(
    const std::vector<std::byte>& bytes);

}  // namespace forgekv::cluster
