#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
preset="${1:-debug}"

cmake --preset "${preset}" -S "${repo_root}"
cmake --build "${repo_root}/build/${preset}" --parallel
