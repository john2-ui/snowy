# Project rules

- Use `ponytail`: prefer existing code, the standard library, and native APIs; add only abstractions needed by working features.
- Keep documentation concise. Use English Doxygen comments for C++ contracts, lifetimes, and non-obvious invariants.
- Use short, familiar names (`task`, `loop`, `socket`, `read`, `write`). Avoid redundant prefixes and speculative configuration.
- Use clang-format 18+ with `.clang-format`; clangd uses the same formatting rules and `.clangd` for editor style. Keep unrelated formatting changes out of commits.
- Keep development logs, milestone reports, temporary measurements, machine-specific paths, and build outputs outside commits. Publish reproducible benchmarks separately in `BENCHMARK.md`.
- Finish each module with meaningful tests. Use `commit-message` to inspect the staged diff and commit locally with a Chinese Conventional Commit subject and concise bullet body. The user has authorized these commits; do not ask again.
- Target C++20 on Linux/io_uring, macOS/kqueue, and Windows/IOCP. Verify native behavior in CI; distinguish local checks from unexecuted CI.
- Provide small runnable examples. Benchmark comparable workloads and settings; consult Condy's `docs/bench.md`. Use profiling to investigate measured regressions before adding optimizations.
- Do not claim performance or platform support beyond verified evidence. Keep buffer and operation lifetime guarantees explicit.
