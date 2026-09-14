# Benchmarks

Inspired by [Condy's benchmark categories](https://github.com/condy-cpp/condy/blob/master/docs/bench.md).
Results from different machines, compilers, backends or workloads are not rankings.

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DSNOWY_BUILD_BENCHMARKS=ON
cmake --build build --config Release --parallel
./build/bench/snowy_runtime task 1000000
./build/bench/snowy_runtime schedule 1000000
./build/bench/snowy_runtime spawn 1000000
./build/bench/snowy_tcp 10000 128
./build/bench/snowy_tcp 10000 2048
./build/bench/snowy_tcp 10000 65536
# Linux only: operations per lane, concurrent lanes
./build/bench/snowy_uring 100000 1
./build/bench/snowy_uring 100000 32
./build/bench/snowy_uring 100000 128
```

With MSVC, executables are under `build/bench/Release` and end in `.exe`.

## Measurement contract

- Runtime/NOP: discard one warmup run, report median/min/max of nine **run
  means** in ns/op. These are not individual-operation latency percentiles.
  Loop and root allocation are excluded; child task allocation is included.
  Disable LTO: `task` intentionally retains a noinline child factory and checksum.
- `spawn` uses batches of 64 roots, including allocation and reclamation;
  `schedule` measures cooperative queue roundtrips on one loop thread.
- TCP: one connection, one outstanding message, client/server on the same loop,
  TCP_NODELAY, up to 1,000 warmup messages. Reports per-message p50/p99/max RTT
  and bidirectional payload MiB/s, excluding setup/teardown. Throughput includes
  timing and payload verification overhead. Repeat the process at least nine
  times; these measurements are not saturation throughput or one-way latency.
- Linux NOP: raw liburing versus Snowy's **internal** awaiter; equal operation
  count, queue depth and 256-entry rings, no SQPOLL. Alternating run order.
  Snowy additionally provides wakeups, cancellation state and a 64-resume
  fairness budget. This isolates wrapper/scheduler cost, not disk or TCP speed.
- Record commit, OS/kernel, CPU, compiler/STL, flags, liburing, affinity and
  power settings alongside stdout. Use the same environment for comparisons;
  do not use shared CI runners for performance claims.

For regressions, use `perf stat`/`perf record` on an optimized build with debug
symbols; `strace -c` can reveal syscall overhead when perf access is restricted.
Keep raw data and profiler outputs outside the repository.

File I/O, channels, multishot receive and multiple loop threads need their own
equivalent workloads before comparisons; the current suite does not measure them.
