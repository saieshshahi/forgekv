#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export TSAN_OPTIONS="${TSAN_OPTIONS:-halt_on_error=1}"

"${repo_root}/scripts/build.sh" tsan

if grep -qi microsoft /proc/sys/kernel/osrelease && command -v setarch >/dev/null 2>&1; then
  setarch "$(uname -m)" -R \
    ctest --test-dir "${repo_root}/build/tsan" --output-on-failure
else
  ctest --test-dir "${repo_root}/build/tsan" --output-on-failure
fi
