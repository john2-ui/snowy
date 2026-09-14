# Snowy

A small C++20 coroutine library with native io_uring (Linux), kqueue (macOS),
and IOCP (Windows) backends. Includes lazy `task<T>`, a single-thread `loop`,
cancellable timers, cross-thread posting, IPv4/IPv6 TCP and UDP, positional
file I/O, bounded channels, structured joins/timeouts, and a fixed worker pool.

```cpp
#include <snowy/snowy.hpp>
#include <iostream>

snowy::task<> hello(snowy::loop& loop) {
    co_await loop.sleep(std::chrono::milliseconds{10});
    std::cout << "Hello\n";
}

int main() {
    snowy::loop loop;
    loop.run(hello(loop));
}
```

## Build

Requires CMake 3.20+, C++20 coroutines and `std::stop_token`. Linux requires
liburing 2.3+ and a kernel with io_uring enabled (tested on 5.15). Clang can
use libstdc++; with libc++, use version 20+. Windows targets MSVC 2022.
Native backend tests run through the GitHub Actions matrix when CI is enabled.

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --config Release --parallel
ctest --test-dir build -C Release --output-on-failure
```

For embedding, use `add_subdirectory(snowy)` and link `snowy::snowy`.
Optional targets: `SNOWY_BUILD_TESTS`, `SNOWY_BUILD_EXAMPLES` (both default ON).
See [timer](examples/timer.cpp) and the self-contained [TCP echo](examples/echo.cpp).
Also see [UDP with timeout](examples/datagram.cpp), [file reads](examples/read.cpp),
a [parallel pipeline](examples/pipeline.cpp), and [cross-thread messages](examples/mailbox.cpp).
Reproducible performance workloads are documented in [BENCHMARK.md](BENCHMARK.md).

## Contracts

- Tasks are lazy and move-only; await them as rvalues. `sync_wait` blocks for
  a task result; it does not drive a loop.
- `task<T, Allocator>` uses an explicit leading allocator argument for its frame;
  `<snowy/pmr.hpp>` provides `pmr::task<T>`. The resource outlives frame destruction
  and must support the threads that allocate/free it. Children choose independently.
- `spawn(loop, task)` returns a move-only `join_handle<T>`: await it on a loop or
  call `get()` outside loop execution. Joining drains through stop; destruction
  or `detach()` discards the result/error but leaves task cleanup owned by the loop.
- Construct, run and destroy a loop on its owner thread. `run()` drains roots
  and posts, returning at idle. `post`, `stop` and `keep_alive` are thread-safe.
  Hold a keep-alive guard before starting an otherwise idle destination loop.
  `co_await target.on()` migrates execution, not I/O ownership; root/join cleanup
  returns to the originating loop. The destination must remain alive until delivery.
- Buffers, sockets and the loop must outlive pending operations. A socket
  allows one read and one write concurrently; do not move it while busy.
- Reads/writes may be partial. Use `write_all` to send a whole buffer.
  Errors throw `std::system_error`; cancellation does not undo partial progress.
- `stop()` permanently cancels pending/future I/O and timers. Root failures
  stop siblings and are rethrown after cleanup; CPU work must yield cooperatively.
- `event` and `channel<T>` are owner-thread primitives. Channels provide bounded
  buffering, zero-capacity rendezvous, `try_send`/`try_recv`, and close/drain;
  `T` must be nothrow move-constructible. Send/receive are direct awaiters, not tasks.
- `mailbox<T>` shares the same FIFO semantics across threads: `send(loop, value)`
  and `recv(loop)` resume on the supplied loop. `futex<T>` awaits an external
  atomic; store before notifying and recheck the predicate after wakeup.
- `when_all` joins a vector of same-result tasks or a typed task pack into a tuple.
  It also accepts stable lvalue awaiters or owned zero-argument factories returning
  direct I/O awaiters. `when_any(loop, factories...)` deduces a variant result;
  explicit `when_any<T>` retains the homogeneous index/value API.
  `when_any`/`timeout` take
  token-aware factories and drain canceled children before returning; timeout is
  cooperative, not a guarantee that cleanup finishes at the deadline. Typed
  factories may be move-only; fixed-count result slots live in the parent frame.
- `pool::run` executes owned CPU/blocking functions on workers and resumes on the
  calling loop. Running jobs are drained on cancellation; I/O does not migrate.
- Files use explicit offsets. Open/close are synchronous; macOS read/write/flush
  and Windows flush use two shared workers. Linux read/write/flush and Windows
  read/write use native completions. Exclusive `mode::create` never truncates.

## Linux fast paths

Explicitly include `<snowy/uring.hpp>`; the portable API does not require it.
See the [registered-file example](examples/fixed.cpp).

- `loop(uring::options)` configures SQ/CQ sizes, scheduling budget, SQPOLL,
  IOPOLL and task-run flags. Requested unsupported modes fail, never silently downgrade.
  `submit_batch` is independent of the resume budget; linked batches stay intact.
  `register_fd` avoids ring-fd lookup and `wq_fd` shares an existing ring's io-wq.
  `workers`/`affinity` tune io-wq, while `napi` configures optional NIC busy polling.
- `uring::memory`, `files` and `buffers` own aligned storage or kernel registrations.
  Registered memory must outlive its table, and tables must outlive every referencing
  operation. `file(..., direct=true)` requests direct I/O on Linux/Windows;
  alignment constraints depend on the filesystem/device. macOS rejects this mode.
- `files(loop, count)` / `buffers(loop, count)` allocate sparse tables; `update`
  replaces idle slots and returns the possibly partial update count. Destroy all
  referencing operations before updating. Direct `files::open/socket/accept/close`
  operate on registered slots, not ordinary descriptors; serialize each slot's
  replacement/close against its I/O. These direct descriptors do not imply `O_DIRECT`.
- `buffers(destination, source)` clones registrations with liburing 2.9+/Linux 6.12+.
  Synchronize source mutation during cloning; underlying memory outlives both tables.
  Cross-thread clone sources require `options.single_issuer = false`.
  Older builds reject cloning/NAPI explicitly instead of silently emulating them.
- `uring::submit` batches one-shot SQEs with soft/hard links and DRAIN, returning
  raw results in input order. DRAIN requires earlier user I/O to finish naturally;
  do not place it behind long-lived receives/polls that need later cancellation.
  It excludes Snowy's internal wake poll; external wake checks are then bounded to 1 ms.
- `uring::accept`/`recv` use multishot requests. `provided::chunk` owns a buffer
  lease; release it on the owner thread. Exhaustion waits for returned buffers.
  Callback exceptions cancel and drain the request before propagating.
- `send_zc` waits for both send completion and any release notification before
  returning; sends may be partial and the kernel may internally copy. This is
  zero-copy TX, **not ZCRX**; zero-copy RX is not implemented.

Advanced features require suitable kernels (multishot RX and SEND_ZC: 6.0+) and
device support where applicable. Opcode probing cannot establish every flag or
hardware prerequisite. IOPOLL rings are storage-only. Native tests report skips
on older kernels; Linux CI requires the advanced networking paths to execute.

Optimization ideas are informed by [Condy](https://github.com/condy-cpp/condy),
including symmetric transfer, coroutine-owned requests and batch processing.
Snowy uses its own implementation and API; no comparative speed claims are implied.
