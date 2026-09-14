/** @file pmr_tests.cpp
 *  @brief Check allocator identity, exceptional cleanup and loop integration. */
#include <snowy/pmr.hpp>
#include <snowy/snowy.hpp>
#include <atomic>
#include <cstdlib>
#include <iostream>

/** @brief Fail in optimized builds too. @param ok Required condition. */
void check(bool ok) { if (!ok) std::abort(); }
/** @brief Count live bytes while preserving the requested allocation alignment. */
struct resource : std::pmr::memory_resource {
    std::atomic<std::size_t> live{0};
    bool reject = false;
    /** @brief Allocate or inject bad_alloc. @param size Bytes. @param alignment Alignment. */
    void* do_allocate(std::size_t size, std::size_t alignment) override {
        if (reject) throw std::bad_alloc();
        auto* value = std::pmr::new_delete_resource()->allocate(size, alignment);
        live += size;
        return value;
    }
    /** @brief Release through the same resource. @param data Allocation.
     *  @param size Original bytes. @param alignment Original alignment. */
    void do_deallocate(void* data, std::size_t size, std::size_t alignment) override {
        live -= size;
        std::pmr::new_delete_resource()->deallocate(data, size, alignment);
    }
    /** @brief Only this resource has this identity. @param other Compared resource. */
    bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }
};
using allocator = std::pmr::polymorphic_allocator<std::byte>;
/** @brief Allocate a lazy value task. @param alloc Frame allocator. @param fail Throw in body. */
#ifdef _MSC_VER
__declspec(noinline)
#else
__attribute__((noinline))
#endif
snowy::pmr::task<int> value([[maybe_unused]] allocator alloc, bool fail = false) {
    if (fail) throw std::runtime_error("body");
    co_return 42;
}
/** @brief Mix default and PMR tasks in one loop. @param alloc Frame allocator. @param loop Owner. */
snowy::pmr::task<> run(allocator alloc, snowy::loop& loop) {
    auto values = co_await snowy::when_all(loop, value(alloc), value(alloc));
    check(std::get<0>(values) == 42 && std::get<1>(values) == 42);
    co_await loop.schedule();
}
/** @brief Verify lifetime and error boundaries. */
int main() {
    try {
        resource memory;
        allocator alloc(&memory);
        { auto unused = value(alloc); }
        check(memory.live == 0);
        check(snowy::sync_wait(value(alloc)) == 42);
        check(memory.live == 0);
        bool caught = false;
        try { snowy::sync_wait(value(alloc, true)); }
        catch (const std::runtime_error&) { caught = true; }
        check(caught && memory.live == 0);
        snowy::loop loop;
        loop.run(run(alloc, loop));
        check(memory.live == 0);
        memory.reject = true;
        caught = false;
        try { auto unused = value(alloc); }
        catch (const std::bad_alloc&) { caught = true; }
        check(caught && memory.live == 0);
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
