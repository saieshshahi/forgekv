#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export TSAN_OPTIONS="${TSAN_OPTIONS:-halt_on_error=1}"

if grep -qi microsoft /proc/sys/kernel/osrelease && command -v clang++ >/dev/null 2>&1; then
  build_dir="${repo_root}/build/tsan-clang"
  cmake -S "${repo_root}" -B "${build_dir}" -G Ninja \
    -DCMAKE_BUILD_TYPE=Debug \
    -DCMAKE_CXX_COMPILER=clang++ \
    -DFORGEKV_SANITIZER=thread \
    -DFORGEKV_GLIBCXX_ASSERTIONS=ON \
    -DFORGEKV_BUILD_BENCHMARKS=OFF
  cmake --build "${build_dir}" --parallel
  setarch "$(uname -m)" -R \
    ctest --test-dir "${build_dir}" --output-on-failure
else
  "${repo_root}/scripts/build.sh" tsan
  ctest --test-dir "${repo_root}/build/tsan" --output-on-failure
fi
