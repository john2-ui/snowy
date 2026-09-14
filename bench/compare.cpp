/** @file compare.cpp
 *  @brief Same-binary Snowy/Condy task, scheduling, and NOP comparison. */
#include "common.hpp"
#include <snowy/snowy.hpp>
#include <condy/coro.hpp>
#include <condy/task.hpp>
#include <condy/sync_wait.hpp>
#include <condy/async_operations.hpp>

/** @brief Preserve child allocation without LTO. @param i Payload. */
__attribute__((noinline)) snowy::task<unsigned> snowy_value(unsigned i) { co_return i; }
/** @brief Preserve child allocation without LTO. @param i Payload. */
__attribute__((noinline)) condy::Coro<unsigned> condy_value(unsigned i) { co_return i; }
/** @brief Run immediate children through the same blocking bridge.
 *  @param count Child count. @param reference Select Condy. @param sum Checksum. */
snowy::task<> tasks(unsigned count, bool reference, std::uint64_t& sum) {
    for (unsigned i = 0; i < count; ++i) {
        if (reference) sum += co_await condy_value(i);
        else sum += co_await snowy_value(i);
    }
}
/** @brief Snowy work lane. @param loop Owner. @param count Operations. @param nop Kernel NOPs. */
snowy::task<> snowy_lane(snowy::loop& loop, unsigned count, bool nop) {
    for (unsigned i = 0; i < count; ++i) {
        if (nop) co_await snowy::detail::io{loop, snowy::detail::opcode::nop};
        else co_await loop.schedule();
    }
}
/** @brief Condy work lane. @param count Operations. @param nop Kernel NOPs. */
condy::Coro<> condy_lane(unsigned count, bool nop) {
    for (unsigned i = 0; i < count; ++i) {
        if (nop) {
            const auto result = co_await condy::async_nop();
            if (result != 0) throw std::runtime_error("Condy NOP failed");
        } else co_await condy::co_switch(condy::current_runtime());
    }
}
/** @brief Time one implementation with root/ring construction excluded.
 *  @param mode Task, schedule, or nop. @param count Operations per lane.
 *  @param depth Lanes. @param reference Select Condy. */
double measure(std::string_view mode, unsigned count, unsigned depth, bool reference) {
    if (mode == "task") {
        std::uint64_t sum = 0;
        auto work = tasks(count, reference, sum);
        const auto start = bench::clock::now();
        snowy::sync_wait(std::move(work));
        const double ns = bench::elapsed(start) / count;
        if (sum != std::uint64_t{count} * (count - 1) / 2) throw std::runtime_error("bad checksum");
        return ns;
    }
    if (reference) {
        condy::RuntimeOptions options;
        options.sq_size(256).event_interval(64);
        condy::Runtime loop(options);
        std::vector<condy::Task<>> jobs;
        for (unsigned i = 0; i < depth; ++i) jobs.push_back(condy::co_spawn(loop, condy_lane(count, mode == "nop")));
        loop.allow_exit();
        const auto start = bench::clock::now();
        loop.run();
        for (auto& job : jobs) job.wait();
        jobs.clear();
        return bench::elapsed(start) / (static_cast<double>(count) * depth);
    }
    snowy::loop loop;
    for (unsigned i = 0; i < depth; ++i) loop.spawn(snowy_lane(loop, count, mode == "nop"));
    const auto start = bench::clock::now();
    loop.run();
    return bench::elapsed(start) / (static_cast<double>(count) * depth);
}
/** @brief Alternate implementation order after a discarded warmup pair.
 *  @param argc Argument count. @param argv Mode, count, queue depth. */
int main(int argc, char** argv) {
    try {
        const std::string_view mode = argc > 1 ? argv[1] : "task";
        const auto count = argc > 2 ? bench::count(argv[2]) : 1'000'000u;
        const auto depth = argc > 3 ? bench::count(argv[3], 128) : 1u;
        if (argc > 4 || (mode != "task" && mode != "schedule" && mode != "nop") || (mode == "task" && depth != 1))
            throw std::invalid_argument("usage: snowy_compare [task|schedule|nop] [count/lane] [depth]");
        std::vector<double> a, b;
        for (unsigned i = 0; i < 10; ++i) {
            double x, y;
            if (i % 2) { x = measure(mode, count, depth, false); y = measure(mode, count, depth, true); }
            else { y = measure(mode, count, depth, true); x = measure(mode, count, depth, false); }
            if (i) { a.push_back(x); b.push_back(y); }
        }
        bench::report("snowy", a); bench::report("condy", b);
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
