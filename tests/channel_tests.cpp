/** @file channel_tests.cpp
 *  @brief Exercise bounded queues, broadcast waits, cancellation, and close. */
#include <snowy/snowy.hpp>
#include <cstdlib>
#include <iostream>

/** @brief Fail in every build type. @param condition Required invariant. */
void check(bool condition) { if (!condition) std::abort(); }
/** @brief Produce with backpressure; last producer closes. @param c Queue.
 *  @param left Remaining producers. */
snowy::task<> produce(snowy::channel<int>& c, int& left) {
    for (int i = 1; i <= 1000; ++i) co_await c.send(i);
    if (!--left) c.close();
}
/** @brief Drain until close. @param c Queue. @param sum Received sum. */
snowy::task<> consume(snowy::channel<int>& c, int& sum) {
    while (auto value = co_await c.recv()) sum += *value;
}
/** @brief Cancel a blocked receiver. @param c Queue. @param stop Stop source. */
snowy::task<> canceled(snowy::channel<int>& c, std::stop_source& stop) {
    bool caught = false;
    try { co_await c.recv(stop.get_token()); }
    catch (const std::system_error& e) { caught = e.code() == std::errc::operation_canceled; }
    check(caught);
}
/** @brief Trigger cancellation on a later tick. @param loop Owner. @param stop Source. */
snowy::task<> cancel(snowy::loop& loop, std::stop_source& stop) {
    co_await loop.schedule();
    stop.request_stop();
}
/** @brief Verify close drains values and rejects new sends. @param c Queue. */
snowy::task<> closed(snowy::channel<int>& c) {
    co_await c.send(42);
    c.close();
    check((co_await c.recv()) == 42);
    check(!(co_await c.recv()));
    bool caught = false;
    try { co_await c.send(0); }
    catch (const std::system_error& e) { caught = e.code() == std::errc::broken_pipe; }
    check(caught);
}
/** @brief Cancel a blocked sender without consuming the buffered value.
 *  @param loop Owner. @param c Capacity-one channel. */
snowy::task<> cancel_send(snowy::loop& loop, snowy::channel<int>& c) {
    co_await c.send(1);
    bool caught = false;
    try {
        co_await snowy::timeout<void>(loop, std::chrono::milliseconds{1},
            [&](std::stop_token token) { return c.send(2, token); });
    } catch (const std::system_error& e) { caught = e.code() == std::errc::timed_out; }
    check(caught && (co_await c.recv()) == 1);
}
/** @brief Run contention and lifecycle checks. */
int main() {
    try {
        snowy::loop loop;
        snowy::channel<int> c(loop, 3);
        int left = 4, sum = 0;
        for (int i = 0; i < 4; ++i) { loop.spawn(produce(c, left)); loop.spawn(consume(c, sum)); }
        loop.run();
        check(sum == 4 * 1000 * 1001 / 2);
        snowy::channel<int> empty(loop, 1);
        for (int i = 0; i < 100; ++i) {
            std::stop_source stop;
            loop.spawn(canceled(empty, stop));
            loop.run(cancel(loop, stop));
        }
        loop.run(cancel_send(loop, empty));
        loop.run(closed(empty));
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
