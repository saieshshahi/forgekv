#include "protocol/parser.h"
#include "protocol/serializer.h"

#include <benchmark/benchmark.h>

#include <cstddef>
#include <cstdint>

namespace forgekv::protocol {
namespace {

Frame make_frame(const std::size_t payload_size) {
  Frame frame{
      .message_namespace = Namespace::client,
      .message_type = MessageType::put,
      .flags = 0U,
      .request_id = 1U,
      .payload = {},
  };
  frame.payload.resize(payload_size, std::byte{0xA5});
  return frame;
}

void BM_ProtocolSerialize(benchmark::State& state) {
  const auto size = static_cast<std::size_t>(state.range(0));
  const auto frame = make_frame(size);
  for (auto _ : state) {
    const auto result = serialize(frame);
    benchmark::DoNotOptimize(result.bytes.data());
    benchmark::ClobberMemory();
  }
  state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(kHeaderSize + size));
}

void BM_ProtocolParse(benchmark::State& state) {
  const auto size = static_cast<std::size_t>(state.range(0));
  const auto wire = serialize(make_frame(size)).bytes;
  for (auto _ : state) {
    Parser parser;
    const auto result = parser.consume(wire);
    benchmark::DoNotOptimize(result.frames.data());
    benchmark::ClobberMemory();
  }
  state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(wire.size()));
}

BENCHMARK(BM_ProtocolSerialize)
    ->Arg(0)
    ->Arg(100)
    ->Arg(1024)
    ->Arg(4096)
    ->Arg(static_cast<std::int64_t>(kMaxPayloadSize));
BENCHMARK(BM_ProtocolParse)
    ->Arg(0)
    ->Arg(100)
    ->Arg(1024)
    ->Arg(4096)
    ->Arg(static_cast<std::int64_t>(kMaxPayloadSize));

}  // namespace
}  // namespace forgekv::protocol
