/** @file timer.cpp
 *  @brief Suspend without blocking the loop thread. */
#include <snowy/snowy.hpp>
#include <iostream>

/** @brief Print after a short delay. @param loop Owner event loop. */
snowy::task<> hello(snowy::loop& loop) {
    co_await loop.sleep(std::chrono::milliseconds{10});
    std::cout << "Hello from Snowy\n";
}

/** @brief Run one task and drain the loop. */
int main() {
    try {
        snowy::loop loop;
        loop.run(hello(loop));
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
