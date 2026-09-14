/** @file runtime.cpp
 *  @brief Measure task, cooperative yield, and bounded root-spawn overhead. */
#include "common.hpp"
#include <snowy/snowy.hpp>
#include <snowy/pmr.hpp>
#include <cstdint>

#if defined(_MSC_VER)
#define SNOWY_NOINLINE __declspec(noinline)
#else
#define SNOWY_NOINLINE __attribute__((noinline))
#endif

/** @brief Keep a task frame observable without LTO. @param value Result payload. */
SNOWY_NOINLINE snowy::task<std::uint64_t> value(std::uint64_t value) { co_return value; }
/** @brief Keep allocator-aware frame creation observable too. @param alloc Frame allocator.
 *  @param value Result payload. */
SNOWY_NOINLINE snowy::pmr::task<std::uint64_t> value(
    [[maybe_unused]] std::pmr::polymorphic_allocator<std::byte> alloc, std::uint64_t value) { co_return value; }

/** @brief Run a task chain with an observable checksum.
 *  @param count Number of child tasks. @param sum Output checksum. */
snowy::task<> tasks(unsigned count, std::uint64_t& sum) {
    for (unsigned i = 0; i < count; ++i) sum += co_await value(i);
}
/** @brief Measure reusable PMR frames on one owner thread. @param pool Frame resource.
 *  @param count Child tasks. @param sum Checksum. */
snowy::task<> tasks(std::pmr::memory_resource& pool, unsigned count, std::uint64_t& sum) {
    for (unsigned i = 0; i < count; ++i) sum += co_await value({&pool}, i);
}

/** @brief Measure owner-thread queue roundtrips.
 *  @param loop Owner. @param count Number of yields. */
snowy::task<> yields(snowy::loop& loop, unsigned count) {
    for (unsigned i = 0; i < count; ++i) co_await loop.schedule();
}

/** @brief Count one root completion. @param done Shared owner-thread counter. */
snowy::task<> child(unsigned& done) { ++done; co_return; }

/** @brief Spawn roots in bounded batches including allocation and reclamation.
 *  @param loop Owner. @param count Total roots. */
snowy::task<> spawn(snowy::loop& loop, unsigned count) {
    for (unsigned base = 0; base < count;) {
        const auto batch = std::min(64u, count - base);
        unsigned done = 0;
        for (unsigned i = 0; i < batch; ++i) loop.spawn(child(done));
        while (done != batch) co_await loop.schedule();
        base += batch;
    }
}

/** @brief Run a warmup plus nine samples; accept workload and operation count. */
int main(int argc, char** argv) {
    try {
        const std::string_view mode = argc > 1 ? argv[1] : "schedule";
        const auto count = argc > 2 ? bench::count(argv[2]) : 1'000'000u;
        if (argc > 3 || (mode != "task" && mode != "pmr" && mode != "schedule" && mode != "spawn"))
            throw std::invalid_argument("usage: snowy_runtime [task|pmr|schedule|spawn] [count]");
        std::vector<double> samples;
        for (unsigned sample = 0; sample < 10; ++sample) {
            snowy::loop loop;
            std::pmr::unsynchronized_pool_resource pool;
            std::uint64_t sum = 0;
            auto work = mode == "task" ? tasks(count, sum) : mode == "pmr" ? tasks(pool, count, sum)
                : mode == "schedule" ? yields(loop, count) : spawn(loop, count);
            loop.spawn(std::move(work));
            const auto start = bench::clock::now();
            loop.run();
            const double ns = bench::elapsed(start) / count;
            if ((mode == "task" || mode == "pmr") && sum != std::uint64_t{count} * (count - 1) / 2)
                throw std::runtime_error("task checksum mismatch");
            if (sample) samples.push_back(ns);
        }
        bench::report(mode, samples);
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
