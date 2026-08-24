#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
preset="${1:-release}"
shift || true

"${repo_root}/scripts/build.sh" "${preset}"
"${repo_root}/build/${preset}/bench/forgekv_benchmarks" "$@"
