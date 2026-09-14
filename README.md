# Snowy

A small C++20 coroutine library with native io_uring (Linux), kqueue (macOS),
and IOCP (Windows) backends. Includes lazy `task<T>`, a single-thread `loop`,
cancellable timers, cross-thread posting, and IPv4/IPv6 TCP sockets.

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
Reproducible performance workloads are documented in [BENCHMARK.md](BENCHMARK.md).

## Contracts

- Tasks are lazy and move-only; await them as rvalues. `sync_wait` blocks for
  a task result; it does not drive a loop.
- Construct, run and destroy a loop on its owner thread. `run()` drains roots
  and posts, returning at idle. Only `post`, `stop` and stop tokens are thread-safe.
- Buffers, sockets and the loop must outlive pending operations. A socket
  allows one read and one write concurrently; do not move it while busy.
- Reads/writes may be partial. Use `write_all` to send a whole buffer.
  Errors throw `std::system_error`; cancellation does not undo partial progress.
- `stop()` permanently cancels pending/future I/O and timers. Root failures
  stop siblings and are rethrown after cleanup; CPU work must yield cooperatively.

Optimization ideas are informed by [Condy](https://github.com/condy-cpp/condy),
including symmetric transfer, coroutine-owned requests and batch processing.
Snowy uses its own implementation and API; no comparative speed claims are implied.
