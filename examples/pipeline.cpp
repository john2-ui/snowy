/** @file pipeline.cpp
 *  @brief Channel backpressure, parallel worker jobs, and structured joining. */
#include <snowy/snowy.hpp>
#include <iostream>

/** @brief Feed ten jobs and close. @param jobs Bounded queue. */
snowy::task<> produce(snowy::channel<int>& jobs) {
    for (int i = 1; i <= 10; ++i) co_await jobs.send(i);
    jobs.close();
}
/** @brief Run CPU work off-loop, then aggregate on the owner thread.
 *  @param loop Owner. @param pool Workers. @param jobs Queue. @param sum Shared loop-local sum. */
snowy::task<> consume(snowy::loop& loop, snowy::pool& pool, snowy::channel<int>& jobs, int& sum) {
    while (auto value = co_await jobs.recv())
        sum += co_await pool.run(loop, [n = *value] { return n * n; });
}
/** @brief Join producer and consumers before destroying their shared state.
 *  @param loop Owner. @param pool Workers. */
snowy::task<> run(snowy::loop& loop, snowy::pool& pool) {
    snowy::channel<int> jobs(loop, 2);
    int sum = 0;
    std::vector<snowy::task<>> tasks;
    tasks.push_back(produce(jobs));
    for (int i = 0; i < 2; ++i) tasks.push_back(consume(loop, pool, jobs, sum));
    co_await snowy::when_all(loop, std::move(tasks));
    if (sum != 385) throw std::runtime_error("pipeline result mismatch");
    std::cout << "Sum of squares: " << sum << '\n';
}
/** @brief Execute a bounded pipeline. */
int main() {
    try { snowy::pool pool(2); snowy::loop loop; loop.run(run(loop, pool)); }
    catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
