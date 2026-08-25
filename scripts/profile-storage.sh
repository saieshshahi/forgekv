#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
workload="${1:-all}"
requested_repetitions="${2:-0}"
output_root="${repo_root}/build/profile-storage"
benchmark_bin="${repo_root}/build/release/bench/forgekv_benchmarks"
flamegraph_dir="${repo_root}/build/tools/FlameGraph"
temporary_files=()

cleanup() {
  if ((${#temporary_files[@]} > 0)); then
    rm -f "${temporary_files[@]}"
  fi
}
trap cleanup EXIT

case "${workload}" in
  all | async-put | sync-put | group-put | get | recovery) ;;
  *)
    echo "unknown workload: ${workload}" >&2
    echo "use: all, async-put, sync-put, group-put, get, recovery" >&2
    exit 2
    ;;
esac

if [[ ! "${requested_repetitions}" =~ ^[0-9]+$ ]]; then
  echo "repetitions must be a non-negative integer" >&2
  exit 2
fi

find_perf() {
  local candidate
  for candidate in "${repo_root}"/build/tools/perf-wsl-*; do
    if [[ -x "${candidate}" ]]; then
      printf '%s\n' "${candidate}"
      return 0
    fi
  done
  for candidate in /usr/lib/linux-tools/*/perf; do
    if [[ -x "${candidate}" ]]; then
      printf '%s\n' "${candidate}"
      return 0
    fi
  done
  if command -v perf >/dev/null 2>&1 && perf --version >/dev/null 2>&1; then
    command -v perf
    return 0
  fi
  return 1
}

perf_bin="$(find_perf)" || {
  echo "perf is unavailable; install linux-tools for this environment" >&2
  exit 2
}

if [[ ! -x "${flamegraph_dir}/stackcollapse-perf.pl" ||
      ! -x "${flamegraph_dir}/flamegraph.pl" ]]; then
  echo "FlameGraph scripts are missing from ${flamegraph_dir}" >&2
  exit 2
fi

"${repo_root}/scripts/build.sh" release
mkdir -p "${output_root}"

profile_one() {
  local name="$1"
  local filter="$2"
  local default_repetitions="$3"
  local repetitions="${requested_repetitions}"
  local output_dir="${output_root}/${name}"
  local perf_data_tmp

  if [[ "${repetitions}" == "0" ]]; then
    repetitions="${default_repetitions}"
  fi
  mkdir -p "${output_dir}"

  "${benchmark_bin}" \
    --benchmark_filter="${filter}" \
    --benchmark_repetitions="${repetitions}" \
    --benchmark_report_aggregates_only=true \
    --benchmark_out="${output_dir}/benchmark.json" \
    --benchmark_out_format=json \
    >"${output_dir}/benchmark.txt" 2>&1

  "${perf_bin}" stat \
    -e task-clock,context-switches,cpu-migrations,page-faults,cycles,instructions,branches,branch-misses,cache-references,cache-misses \
    -o "${output_dir}/perf-stat.txt" -- \
    "${benchmark_bin}" --benchmark_filter="${filter}" \
    --benchmark_repetitions="${repetitions}" \
    --benchmark_report_aggregates_only=true >/dev/null 2>&1

  # perf uses a memory-mapped output file. Recording directly onto DrvFS
  # fails with EFAULT under WSL, so record natively and copy after close.
  perf_data_tmp="$(mktemp "/tmp/forgekv-${name}-perf.XXXXXX.data")"
  temporary_files+=("${perf_data_tmp}")
  "${perf_bin}" record -e cpu-clock -F 199 -g --call-graph dwarf \
    -o "${perf_data_tmp}" -- \
    "${benchmark_bin}" --benchmark_filter="${filter}" \
    --benchmark_repetitions="${repetitions}" \
    --benchmark_report_aggregates_only=true >/dev/null 2>&1
  "${perf_bin}" script -i "${perf_data_tmp}" \
    >"${output_dir}/perf-script.txt"
  cp "${perf_data_tmp}" "${output_dir}/perf.data"
  rm -f "${perf_data_tmp}"
  "${flamegraph_dir}/stackcollapse-perf.pl" \
    "${output_dir}/perf-script.txt" >"${output_dir}/stacks.folded"
  "${flamegraph_dir}/flamegraph.pl" --title "ForgeKV ${name}" \
    "${output_dir}/stacks.folded" >"${output_dir}/flamegraph.svg"

  strace -f -c -o "${output_dir}/strace-summary.txt" \
    "${benchmark_bin}" --benchmark_filter="${filter}" \
    --benchmark_repetitions=1 >/dev/null 2>&1

  valgrind --tool=massif --stacks=no \
    --massif-out-file="${output_dir}/massif.out" \
    "${benchmark_bin}" --benchmark_filter="${filter}" \
    --benchmark_repetitions=1 >/dev/null 2>&1
  ms_print "${output_dir}/massif.out" >"${output_dir}/massif.txt"
}

run_named() {
  case "$1" in
    async-put)
      profile_one async-put '^StoragePut/4096/64/' 1000
      ;;
    sync-put)
      profile_one sync-put '^StorageDurabilityPut/mode_0_async_1_sync_2_group:1/value_bytes:4096/' 100
      ;;
    group-put)
      profile_one group-put '^StorageDurabilityPut/mode_0_async_1_sync_2_group:2/value_bytes:4096/' 100
      ;;
    get)
      profile_one get '^StorageGet/4096/64/' 5000
      ;;
    recovery)
      profile_one recovery '^StorageRecovery/1048576/64/' 10
      ;;
    *)
      echo "unknown workload: $1" >&2
      echo "use: all, async-put, sync-put, group-put, get, recovery" >&2
      exit 2
      ;;
  esac
}

if [[ "${workload}" == "all" ]]; then
  run_named async-put
  run_named sync-put
  run_named group-put
  run_named get
  run_named recovery
else
  run_named "${workload}"
fi

echo "storage profiles written to ${output_root}"
