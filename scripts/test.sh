#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
preset="${1:-debug}"

"${repo_root}/scripts/build.sh" "${preset}"
ctest --test-dir "${repo_root}/build/${preset}" --output-on-failure
