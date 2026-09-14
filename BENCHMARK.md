# Benchmarks

Inspired by [Condy's benchmark categories](https://github.com/condy-cpp/condy/blob/master/docs/bench.md).
Results from different machines, compilers, backends or workloads are not rankings.

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DSNOWY_BUILD_BENCHMARKS=ON
cmake --build build --config Release --parallel
./build/bench/snowy_runtime task 1000000
./build/bench/snowy_runtime schedule 1000000
./build/bench/snowy_runtime spawn 1000000
./build/bench/snowy_channel 1000000 1024
./build/bench/snowy_file /path/to/existing-file 10000 32 random 4096
./build/bench/snowy_tcp 10000 128
./build/bench/snowy_tcp 10000 2048
./build/bench/snowy_tcp 10000 65536
# Linux only: operations per lane, concurrent lanes
./build/bench/snowy_uring 100000 1
./build/bench/snowy_uring 100000 32
./build/bench/snowy_uring 100000 128
# Linux: identical raw/wrapped random reads; path must already exist
./build/bench/snowy_storage fixed-direct /path/to/existing-file 10000 32 4096
./build/bench/snowy_storage iopoll /path/to/existing-file 10000 32 4096
# Continuous loopback receive with an independent sender thread
./build/bench/snowy_rx single 10000 2048
./build/bench/snowy_rx multi 10000 2048 # Linux 6.0+ provided-buffer rings
./build/bench/snowy_rx bundle 10000 2048 # Linux 6.10+, newer liburing
```

With MSVC, executables are under `build/bench/Release` and end in `.exe`.

## Measurement contract

- Runtime/NOP: discard one warmup run, report median/min/max of nine **run
  means** in ns/op. These are not individual-operation latency percentiles.
  Loop and root allocation are excluded; child task allocation is included.
  Disable LTO: `task` intentionally retains a noinline child factory and checksum.
- `spawn` uses batches of 64 roots, including allocation and reclamation;
  `schedule` measures cooperative queue roundtrips on one loop thread.
- Channel: one producer/consumer on one loop, configurable capacity, verified
  order/count. Units are ns/message (send + receive), not ns per API call.
- File: read-only buffered I/O, preallocated per-lane buffers, deterministic
  random or interleaved sequential offsets. Open/close and root allocation are
  excluded. Use a non-sparse file of at least 8 GiB for storage tests; cache state,
  filesystem and backing device must be reported. Warm cached reads are not SSD IOPS.
- TCP: one connection, one outstanding message, client/server on the same loop,
  TCP_NODELAY, up to 1,000 warmup messages. Reports per-message p50/p99/max RTT
  and bidirectional payload MiB/s, excluding setup/teardown. Throughput includes
  timing and payload verification overhead. Repeat the process at least nine
  times; these measurements are not saturation throughput or one-way latency.
- Linux NOP: raw liburing versus Snowy's **internal** awaiter; equal operation
  count, queue depth and 256-entry rings, no SQPOLL. Alternating run order.
  Snowy additionally provides wakeups, cancellation state and a 64-resume
  fairness budget. This isolates wrapper/scheduler cost, not disk or TCP speed.
- Storage: `buffered`, `direct`, `fixed`, `fixed-direct`, `iopoll`, `sqpoll`.
  Fixed modes register both files and buffers; polling modes also use direct I/O.
  Raw/wrapped pairs share offsets, depth, ring flags and buffer sizes; allocation,
  open and registration are excluded. Report nine alternating run means after
  warmup. Direct modes require 4096-byte multiples and filesystem/device support;
  unsupported modes fail explicitly. No file is created, modified or cache-evicted.
- RX: single-shot or Linux multishot with 256 provided buffers, one blocking
  sender thread, nine samples after warmup. Reports median/min/max payload MiB/s;
  byte validation and EOF are timed, accept and buffer allocation are not.
  Run both modes repeatedly with matching sizes; loopback is not NIC throughput.
- Record commit, OS/kernel, CPU, compiler/STL, flags, liburing, affinity and
  power settings alongside stdout. Use the same environment for comparisons;
  do not use shared CI runners for performance claims.

For regressions, use `perf stat`/`perf record` on an optimized build with debug
symbols; `strace -c` can reveal syscall overhead when perf access is restricted.
Keep raw data and profiler outputs outside the repository.

## Condy comparison

Provide a separate upstream checkout; Snowy does not vendor or patch Condy.
The CI reference is `01dcf995b711e67ac4416a07fdfe6347a388ad14`.

```sh
cmake -S . -B build -DSNOWY_BUILD_BENCHMARKS=ON \
  -DSNOWY_CONDY_INCLUDE_DIR=/path/to/condy/include
cmake --build build --parallel
./build/bench/snowy_compare task 1000000
./build/bench/snowy_compare schedule 1000000
./build/bench/snowy_compare nop 10000 32
./build/bench/snowy_compare_storage fixed-direct /path/to/existing-file 10000 32 4096
./build/bench/snowy_compare_rx single 10000 2048
./build/bench/snowy_compare_rx multi 10000 2048
```

Both implementations share one binary, compiler/STL and sample procedure.
Task mode uses the same blocking bridge, noinline child factories and checksum;
it measures immediate coroutine calls, not the runtimes. Schedule/NOP use each
library's own runtime, 256-entry rings and a 64-work event interval. Condy's ring
registration/setup policies remain upstream defaults, not identical backend flags.
These modes require a kernel supporting upstream Condy's io_uring flags; task
mode also runs on older kernels. With liburing 2.3, explicitly select its static
archive via `URING_LIBRARY` (its shared library omits `io_uring_enable_rings`).
The reference also requires a standard library with `std::format`.

Storage comparison reuses all six storage modes, identical offsets, aligned buffers,
depth and checksums. RX comparison reuses the same independent sender, payload
validation and 256-buffer single/multishot workloads. Both alternate library order
after warmup; setup/registration and accept are excluded. Runtime setup/registration
policies remain each library's own. Buffer bundles are Snowy-only in this comparison.

`snowy_runtime pmr 1000000` measures non-elided frames using a standard unsynchronized
pool; compare with `task` under the same compiler/STL. Pool reuse is not necessarily
faster than the default allocator. `snowy_mailbox 100000 1024` measures two-loop
delivery including wakeups, FIFO validation and thread teardown; capacity `0`
selects rendezvous. Setup is excluded by a shared start signal. Neither benchmark
is a Condy comparison.

CI executes comparisons as smoke tests only; publish rankings only after repeated
runs on controlled hardware. Zero-copy send throughput is not yet measured here.
