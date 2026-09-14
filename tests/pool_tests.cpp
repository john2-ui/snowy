/** @file pool_tests.cpp
 *  @brief Check parallel execution, origin resumption, errors, and safe cancellation. */
#include <snowy/snowy.hpp>
#include <cstdlib>
#include <iostream>

/** @brief Fail regardless of NDEBUG. @param ok Required invariant. */
void check(bool ok) { if (!ok) std::abort(); }
/** @brief Run a barrier job; both workers must enter concurrently.
 *  @param loop Owner. @param pool Workers. @param entered Barrier counter. */
snowy::task<> parallel(snowy::loop& loop, snowy::pool& pool, std::atomic_int& entered) {
    const auto origin = std::this_thread::get_id();
    auto value = co_await pool.run(loop, [&, origin] {
        check(std::this_thread::get_id() != origin);
        entered.fetch_add(1);
        while (entered.load() != 2) std::this_thread::yield();
        return std::make_unique<int>(42);
    });
    check(*value == 42 && std::this_thread::get_id() == origin);
}
/** @brief Validate worker exceptions and stop-before-submit. @param loop Owner. @param pool Workers. */
snowy::task<> errors(snowy::loop& loop, snowy::pool& pool) {
    bool caught = false;
    try { co_await pool.run(loop, [] { throw std::runtime_error("worker"); }); }
    catch (const std::runtime_error&) { caught = true; }
    check(caught);
    std::stop_source stop;
    stop.request_stop();
    caught = false;
    try { co_await pool.run(loop, [] { std::abort(); }, stop.get_token()); }
    catch (const std::system_error& e) { caught = e.code() == std::errc::operation_canceled; }
    check(caught);
}
/** @brief Stop while a worker owns frame references; completion must be drained.
 *  @param loop Owner. @param pool Workers. */
snowy::task<> stopping(snowy::loop& loop, snowy::pool& pool) {
    bool finished = false, caught = false;
    try {
        co_await pool.run(loop, [&] {
            loop.stop();
            std::this_thread::sleep_for(std::chrono::milliseconds{2});
            finished = true;
        });
    } catch (const std::system_error& e) { caught = e.code() == std::errc::operation_canceled; }
    check(caught && finished);
}
/** @brief Run lifecycle checks. */
int main() {
    try {
        snowy::pool pool(2);
        snowy::loop loop;
        std::atomic_int entered{0};
        loop.spawn(parallel(loop, pool, entered));
        loop.run(parallel(loop, pool, entered));
        loop.run(errors(loop, pool));
        loop.run(stopping(loop, pool));
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
