# Snowy

A small C++20 coroutine library. `task<T>` is lazy and move-only; `sync_wait`
blocks for its result and propagates exceptions. Await tasks as rvalues.

```cpp
#include <snowy/snowy.hpp>

snowy::task<int> answer() { co_return 42; }
int main() { return snowy::sync_wait(answer()) == 42 ? 0 : 1; }
```

Build with CMake 3.20+ and a C++20 compiler:

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

CI targets GCC 14, Clang 18, AppleClang, and MSVC 2022.
