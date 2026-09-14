/** @file spawn_tests.cpp
 *  @brief Spawn ownership, cross-thread results, failures and shutdown joins. */
#include <snowy/snowy.hpp>
#include <cstdlib>
#include <future>
#include <iostream>

/** @brief Assert in every build. @param ok Required condition. */
void check(bool ok) { if (!ok) std::abort(); }
/** @brief Return a move-only value after yielding. @param loop Owner. @param fail Throw instead. */
snowy::task<std::unique_ptr<int>> value(snowy::loop& loop, bool fail = false) {
    co_await loop.schedule();
    if (fail) throw std::runtime_error("child");
    co_return std::make_unique<int>(42);
}
/** @brief Return void. @param loop Owner. */
snowy::task<> empty(snowy::loop& loop) { co_await loop.schedule(); }
/** @brief Verify single consumption and exceptional publication. @param loop Owner. */
snowy::task<> run(snowy::loop& loop) {
    auto result = snowy::spawn(loop, value(loop));
    check(!result.ready());
    check(*(co_await std::move(result)) == 42 && !result);
    bool caught = false;
    try { co_await std::move(result); }
    catch (const std::logic_error&) { caught = true; }
    check(caught);
    auto failure = snowy::spawn(loop, value(loop, true));
    caught = false;
    try { co_await std::move(failure); }
    catch (const std::runtime_error&) { caught = true; }
    check(caught && !loop.stopped());
    auto done = snowy::spawn(loop, empty(loop));
    co_await std::move(done);
    snowy::spawn(loop, value(loop, true)).detach();
    auto shutdown = snowy::spawn(loop, value(loop));
    loop.stop();
    check(*(co_await std::move(shutdown)) == 42);
}
/** @brief Join a handle produced on another thread. @param handle Transferred result. */
snowy::task<> cross(snowy::join_handle<std::unique_ptr<int>> handle) {
    check(*(co_await std::move(handle)) == 42);
}
/** @brief Test blocking and asynchronous foreign consumers. @param block Use blocking get. */
void foreign(bool block) {
    std::promise<snowy::join_handle<std::unique_ptr<int>>> ready;
    std::jthread producer([&] {
        snowy::loop loop;
        ready.set_value(snowy::spawn(loop, value(loop)));
        loop.run();
    });
    auto result = ready.get_future().get();
    if (block) check(*std::move(result).get() == 42);
    else { snowy::loop loop; loop.run(cross(std::move(result))); }
}
/** @brief Check all handle lifetime paths on a native backend. */
int main() {
    try {
        snowy::loop loop;
        loop.run(run(loop));
        foreign(false);
        foreign(true);
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
