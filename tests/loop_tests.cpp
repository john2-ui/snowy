/** @file loop_tests.cpp
 *  @brief Exercise native wakeups, timer cancellation, and scheduler fairness. */
#include <snowy/snowy.hpp>
#include <snowy/detail/io.hpp>
#include <atomic>
#include <cstdlib>
#include <iostream>

using namespace std::chrono_literals;

/** @brief Fail in every build mode. @param value Expected condition. */
void check(bool value) { if (!value) std::abort(); }

/** @brief Exercise timer, posted work, and a kernel round trip.
 *  @param loop Owner loop. @param count Shared owner-thread counter. */
snowy::task<> run(snowy::loop& loop, int& count) {
    const auto start = snowy::loop::clock::now();
    loop.post([&count] { ++count; });
    co_await loop.schedule();
    co_await loop.sleep(2ms);
    check(snowy::loop::clock::now() >= start + 2ms);
    check(count == 1);
    co_await snowy::detail::io{loop, snowy::detail::opcode::nop};
    std::stop_source stop;
    stop.request_stop();
    bool canceled = false;
    try { co_await loop.sleep(1h, stop.get_token()); }
    catch (const std::system_error& e) { canceled = e.code() == std::errc::operation_canceled; }
    check(canceled);
}

/** @brief Suspend on a long timer until another thread cancels it.
 *  @param loop Owner. @param stop Cancellation source. @param started Publication flag. */
snowy::task<> cancel_timer(snowy::loop& loop, std::stop_source& stop, std::atomic_bool& started) {
    started.store(true, std::memory_order_release);
    bool canceled = false;
    try { co_await loop.sleep(1h, stop.get_token()); }
    catch (const std::system_error& e) { canceled = e.code() == std::errc::operation_canceled; }
    check(canceled);
}

/** @brief Throw inside a root to verify draining before rethrow. */
snowy::task<> fail() {
    co_await std::suspend_never{};
    throw std::runtime_error("root");
}

/** @brief Root whose cleanup requires stop to cancel a long timer.
 *  @param loop Owner. @param cleaned Cleanup indicator. */
snowy::task<> sibling(snowy::loop& loop, bool& cleaned) {
    try { co_await loop.sleep(1h); }
    catch (const std::system_error& e) {
        check(e.code() == std::errc::operation_canceled);
        cleaned = true;
    }
}

/** @brief Keep the ready queue nonempty until a timer and a post both run.
 *  @param loop Owner. @param timed Timer flag. @param posted Post flag. */
snowy::task<> busy(snowy::loop& loop, bool& timed, bool& posted) {
    while (!timed || !posted) co_await loop.schedule();
}

/** @brief Ensure a busy ready queue does not starve deadlines.
 *  @param loop Owner. @param timed Completion flag. */
snowy::task<> deadline(snowy::loop& loop, bool& timed) {
    co_await loop.sleep(2ms);
    timed = true;
}

/** @brief Verify loop contracts. @return Zero on success; nonzero on backend failure. */
int main() {
    try {
        snowy::loop loop;
        int count = 0;
        loop.run(run(loop, count));
        for (int i = 0; i < 100; ++i) {
            std::stop_source stop;
            std::atomic_bool started{false};
            std::thread producer([&] {
                while (!started.load(std::memory_order_acquire)) std::this_thread::yield();
                loop.post([&count] { ++count; });
                stop.request_stop();
            });
            loop.run(cancel_timer(loop, stop, started));
            producer.join();
            loop.run(); // Drain a post that raced with idle exit.
        }
        check(count == 101);
        bool timed = false, posted = false;
        loop.spawn(busy(loop, timed, posted));
        loop.spawn(deadline(loop, timed));
        std::thread poster([&] {
            std::this_thread::sleep_for(1ms);
            loop.post([&posted] { posted = true; });
        });
        loop.run();
        poster.join();
        check(timed && posted);
        bool cleaned = false, caught = false;
        loop.spawn(sibling(loop, cleaned));
        loop.spawn(fail());
        try { loop.run(); }
        catch (const std::runtime_error&) { caught = true; }
        check(caught && cleaned);
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
