# Phase 1 Engineering Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Establish a reproducible C++20 build, test, benchmark, sanitizer, CI, and logging foundation without networking, Raft, or storage behavior.

**Architecture:** CMake defines focused libraries and opt-in hardening/sanitizer settings. Bash scripts are thin, exact wrappers over CMake presets. Logging is a small dependency-free library with compile-time elimination and a runtime severity threshold.

**Tech Stack:** C++20, CMake 3.22+, Ninja, GCC/Clang, GoogleTest 1.15.2, Google Benchmark 1.9.1, GitHub Actions.

**Spec:** `docs/architecture.md`

## Global Constraints

- Target Linux; warnings are `-Wall -Wextra -Wpedantic -Wconversion -Wshadow`.
- ASan, UBSan, and TSan configurations must build independently.
- `FORGEKV_GLIBCXX_ASSERTIONS` enables `_GLIBCXX_ASSERTIONS`.
- CI must call the same scripts used locally.
- Do not add networking, Raft, or storage code in this phase.

---

### Task 1: Build and dependency skeleton

**Files:** Create `CMakeLists.txt`, `cmake/ForgeKVOptions.cmake`, `cmake/Dependencies.cmake`, `CMakePresets.json`, `.gitignore`, `src/CMakeLists.txt`, `tests/CMakeLists.txt`, `tests/unit/smoke_test.cpp`, `bench/CMakeLists.txt`, `bench/micro/smoke_benchmark.cpp`.

**Interfaces:** Produces CMake targets `forgekv_common`, `forgekv_unit_tests`, and `forgekv_benchmarks` and presets `debug`, `release`, `asan`, `ubsan`, `tsan`.

- [x] Add the smoke test before any library implementation; it asserts the test harness runs.
- [x] Add pinned FetchContent declarations with benchmarks disabled from self-testing.
- [x] Configure C++20, warning interface target, sanitizer options, and hardened option.
- [x] Configure from an empty `build/debug` directory and run the smoke test.
- [x] Configure release and execute one benchmark iteration.

### Task 2: Script and CI parity

**Files:** Create `scripts/build.sh`, `scripts/test.sh`, `scripts/test-asan.sh`, `scripts/test-ubsan.sh`, `scripts/test-tsan.sh`, `scripts/bench.sh`, `.github/workflows/ci.yml`.

**Interfaces:** Scripts accept an optional preset/build directory only where documented; all locate the repository relative to the script path.

- [x] Write shell scripts using `set -euo pipefail` and CMake presets.
- [x] Make scripts executable and run `scripts/build.sh` and `scripts/test.sh` in WSL.
- [x] Add GitHub Actions jobs for GCC debug/release and sanitizer tests, all invoking scripts.
- [x] Run ASan, UBSan, and TSan scripts and record environmental limitations, if any, without weakening CI.

### Task 3: Logging abstraction using TDD

**Files:** Create `src/common/logging.h`, `src/common/logging.cpp`, `tests/unit/logging_test.cpp`; modify `src/CMakeLists.txt`, `tests/CMakeLists.txt`.

**Interfaces:** Produces `forgekv::common::Severity`, `Logger::set_runtime_minimum`, `Logger::runtime_minimum`, `Logger::enabled`, `Logger::write`, and `FORGEKV_LOG(severity, message)`.

- [x] Write tests proving severity ordering, runtime filtering, stable severity names, and sink output.
- [x] Build the targeted test and verify RED because logging symbols do not exist.
- [x] Implement the minimal thread-safe logger with an injectable sink and compile-time minimum macro.
- [x] Run targeted and full tests; refactor only while green.

### Task 4: Exact developer documentation

**Files:** Create `README.md`; modify `docs/architecture.md` only to remove the existing Markdown trailing-space warning.

- [x] Document Ubuntu dependencies, WSL usage, clean builds, every script, sanitizer constraints, targets, and repository layout.
- [x] Run `git diff --check`, clean-build debug/release, unit tests, benchmark smoke, and all sanitizer configurations.
- [x] Commit Phase 1 with its plans and push `main`.
