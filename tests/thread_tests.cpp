/** @file thread_tests.cpp
 *  @brief Cross-loop FIFO, migration and cancellation/delivery races. */
#include <snowy/snowy.hpp>
#include <cstdlib>
#include <future>
#include <iostream>

/** @brief Keep assertions enabled in Release. @param ok Required condition. */
void check(bool ok) { if (!ok) std::abort(); }
/** @brief Publish a disjoint range. @param loop Owner. @param box Shared queue.
 *  @param id Producer number. @param remaining Producers still active. */
snowy::task<> produce(snowy::loop& loop, snowy::mailbox<int>& box, int id, std::atomic<int>& remaining) {
    for (int i = 0; i < 10000; ++i) co_await box.send(loop, id * 10000 + i);
    if (--remaining == 0) box.close();
}
/** @brief Count each value exactly once. @param loop Owner. @param box Shared queue.
 *  @param seen Per-value counters. */
snowy::task<> consume(snowy::loop& loop, snowy::mailbox<int>& box, std::vector<std::atomic<int>>& seen) {
    while (auto value = co_await box.recv(loop)) {
        check(*value >= 0 && *value < 20000);
        ++seen[static_cast<std::size_t>(*value)];
        check(snowy::loop::current() == &loop);
    }
}
/** @brief Race two producers against two consumers. @param capacity Queue slots. */
void transfer(std::size_t capacity) {
    snowy::mailbox<int> box(capacity);
    std::atomic<int> remaining{2};
    std::vector<std::atomic<int>> seen(20000);
    for (auto& count : seen) count = 0;
    std::vector<std::jthread> threads;
    for (int i = 0; i < 4; ++i) threads.emplace_back([&, i] {
        snowy::loop loop;
        if (i < 2) loop.run(produce(loop, box, i, remaining));
        else loop.run(consume(loop, box, seen));
    });
    for (auto& thread : threads) thread.join();
    for (auto& count : seen) check(count == 1);
    int value = 1;
    check(!box.try_send(value) && !box.try_recv());
}
/** @brief Leave a child on a foreign loop. @param target Destination. @param fail Throw there. */
snowy::task<int> migrate(snowy::loop& target, bool fail = false) {
    co_await target.on();
    target.check();
    if (fail) throw std::runtime_error("foreign failure");
    co_return 42;
}
/** @brief Verify parent join affinity after child migration. @param origin Owner.
 *  @param target Foreign loop. */
snowy::task<> joined(snowy::loop& origin, snowy::loop& target) {
    for (int i = 0; i < 500; ++i) {
        auto values = co_await snowy::when_all(origin, migrate(target), migrate(target));
        origin.check();
        check(std::get<0>(values) == 42 && std::get<1>(values) == 42);
    }
    bool caught = false;
    try { co_await snowy::when_all(origin, migrate(target, true), migrate(target)); }
    catch (const std::runtime_error&) { caught = true; }
    origin.check();
    check(caught);
}
/** @brief End a root on another loop. @param target Destination. @param fail Throw there. */
snowy::task<> root(snowy::loop& target, bool fail) { co_await migrate(target, fail); }
/** @brief Check root accounting, join cleanup and idle keep-alive. */
void migration() {
    std::promise<snowy::loop*> ready;
    std::jthread thread([&] {
        snowy::loop target;
        auto keep = target.keep_alive();
        ready.set_value(&target);
        target.run();
        check(snowy::loop::current() == nullptr);
    });
    auto& target = *ready.get_future().get();
    snowy::loop origin;
    origin.run(joined(origin, target));
    origin.run(root(target, false));
    bool caught = false;
    try { origin.run(root(target, true)); }
    catch (const std::runtime_error&) { caught = true; }
    check(caught && snowy::loop::current() == nullptr);
    target.stop();
    thread.join();
}
/** @brief Exercise cancellation against foreign delivery. @param loop Owner loop. */
snowy::task<> races(snowy::loop& loop) {
    std::atomic<int> value{0};
    snowy::futex wake(value);
    snowy::mailbox<int> box(0);
    for (int i = 0; i < 300; ++i) {
        std::stop_source stop;
        value = 0;
        std::jthread thread([&] {
            if (i % 2) stop.request_stop();
            value.store(1, std::memory_order_release);
            wake.notify_all();
            stop.request_stop();
        });
        try { co_await wake.wait(loop, 0, stop.get_token()); }
        catch (const std::system_error& e) { check(e.code() == std::errc::operation_canceled); }
        thread.join();
        std::stop_source cancel;
        std::jthread sender([&] {
            int v = i;
            if (i % 2) cancel.request_stop();
            box.try_send(v);
            cancel.request_stop();
        });
        try { auto v = co_await box.recv(loop, cancel.get_token()); check(v && *v == i); }
        catch (const std::system_error& e) { check(e.code() == std::errc::operation_canceled); }
        sender.join();
    }
    box.close();
    bool caught = false;
    try { co_await box.send(loop, 1); }
    catch (const std::system_error& e) { caught = e.code() == std::errc::broken_pipe; }
    check(caught && !(co_await box.recv(loop)));
}
/** @brief Run native cross-thread tests with finite CTest timeout. */
int main() {
    try {
        transfer(0);
        transfer(7);
        migration();
        snowy::loop loop;
        loop.run(races(loop));
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
