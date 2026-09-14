/** @file when_tests.cpp
 *  @brief Validate ordered joins, winning errors, cancellation, and cleanup barriers. */
#include <snowy/snowy.hpp>
#include <cstdlib>
#include <iostream>
using namespace std::chrono_literals;

/** @brief Fail in all builds. @param ok Required invariant. */
void check(bool ok) { if (!ok) std::abort(); }
/** @brief Return an owned result after yielding. @param loop Owner. @param value Payload. */
snowy::task<std::unique_ptr<int>> item(snowy::loop& loop, int value) {
    co_await loop.schedule();
    co_return std::make_unique<int>(value);
}
/** @brief Mark destruction even on cancellation. */
struct cleanup {
    bool& done;
    /** @brief Publish that the child frame has released its local resources. */
    ~cleanup() { done = true; }
};
/** @brief Long-running, token-aware child. @param loop Owner. @param token Stop token.
 *  @param done Cleanup indicator. */
snowy::task<int> slow(snowy::loop& loop, std::stop_token token, bool& done) {
    cleanup guard{done};
    co_await loop.sleep(1h, token);
    co_return 1;
}
/** @brief Complete on the next tick. @param loop Owner. */
snowy::task<int> quick(snowy::loop& loop) { co_await loop.schedule(); co_return 42; }
/** @brief Fail after suspension. @param loop Owner. */
snowy::task<> fail(snowy::loop& loop) {
    co_await loop.schedule();
    throw std::runtime_error("child");
}
/** @brief Complete after a peer fails. @param loop Owner. @param done Cleanup flag. */
snowy::task<> finish(snowy::loop& loop, bool& done) {
    co_await loop.sleep(1ms);
    done = true;
}
/** @brief Run successful and exceptional compositions. @param loop Owner. */
snowy::task<> run(snowy::loop& loop) {
    std::vector<snowy::task<std::unique_ptr<int>>> tasks;
    for (int i = 0; i < 64; ++i) tasks.push_back(item(loop, i));
    auto values = co_await snowy::when_all(loop, std::move(tasks));
    for (int i = 0; i < 64; ++i) check(*values[i] == i);
    std::vector<snowy::task<>> empty;
    check((co_await snowy::when_all(loop, std::move(empty))).empty());
    check((co_await snowy::when_all(loop)) == std::tuple<>{});
    bool done = false;
    auto mixed = co_await snowy::when_all(loop, item(loop, 7), quick(loop), finish(loop, done));
    check(*std::get<0>(mixed) == 7 && std::get<1>(mixed) == 42 && done);
    done = false;
    std::vector<std::function<snowy::task<int>(std::stop_token)>> jobs;
    jobs.push_back([&](std::stop_token t) { return slow(loop, t, done); });
    jobs.push_back([&](std::stop_token) { return quick(loop); });
    auto result = co_await snowy::when_any<int>(loop, std::move(jobs));
    check(result.first == 1 && result.second == 42 && done);
    done = false;
    bool caught = false;
    try {
        co_await snowy::timeout<int>(loop, 1ms,
            [&](std::stop_token t) { return slow(loop, t, done); });
    } catch (const std::system_error& e) { caught = e.code() == std::errc::timed_out; }
    check(caught && done);
    done = false;
    caught = false;
    try { co_await snowy::when_all(loop, fail(loop), finish(loop, done), item(loop, 8)); }
    catch (const std::runtime_error&) { caught = true; }
    check(caught && done);
    check((co_await snowy::timeout<int>(loop, 1h,
        [&](std::stop_token) { return quick(loop); })) == 42);
    done = false;
    jobs.clear();
    jobs.push_back([&](std::stop_token t) { return slow(loop, t, done); });
    jobs.push_back([](std::stop_token) -> snowy::task<int> { throw std::runtime_error("factory"); });
    caught = false;
    try { co_await snowy::when_any<int>(loop, std::move(jobs)); }
    catch (const std::runtime_error&) { caught = true; }
    check(caught && done);
    done = false;
    caught = false;
    empty.clear();
    empty.push_back(fail(loop));
    empty.push_back(finish(loop, done));
    try { co_await snowy::when_all(loop, std::move(empty)); }
    catch (const std::runtime_error&) { caught = true; }
    check(caught && done);
    jobs.clear();
    caught = false;
    try { co_await snowy::when_any<int>(loop, std::move(jobs)); }
    catch (const std::invalid_argument&) { caught = true; }
    check(caught);
    check((co_await snowy::timeout<int>(loop, 1h,
        [value = std::make_unique<int>(7), &loop](std::stop_token) {
            check(*value == 7); return quick(loop);
        })) == 42);
}
/** @brief Run tests under a real backend. */
int main() {
    try { snowy::loop loop; loop.run(run(loop)); }
    catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
