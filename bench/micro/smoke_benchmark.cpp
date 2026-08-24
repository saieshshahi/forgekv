#include <benchmark/benchmark.h>

namespace {

void BM_HarnessOverhead(benchmark::State& state) {
  for (auto _ : state) {
    benchmark::DoNotOptimize(_);
  }
}

BENCHMARK(BM_HarnessOverhead);

}  // namespace
