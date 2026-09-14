/** @file frame.hpp
 *  @brief Optional allocator storage trailing a coroutine frame. */
#pragma once
#include <cstddef>
#include <limits>
#include <memory>
#include <type_traits>

namespace snowy::detail {
/** @brief Save an allocator independently of coroutine argument destruction.
 *  @tparam A Allocator with a nonthrowing copy constructor and raw pointers. */
template <typename A>
struct frame {
    static_assert(std::is_nothrow_copy_constructible_v<A>);
    struct alignas(A) alignas(std::max_align_t) block { std::byte data; };
    using allocator = typename std::allocator_traits<A>::template rebind_alloc<block>;
    using traits = std::allocator_traits<allocator>;
    static_assert(std::is_same_v<typename traits::pointer, block*>);
    /** @brief Round the frame to allocator-storage alignment. @param size Frame bytes. */
    static std::size_t slots(std::size_t size) noexcept { return (size + sizeof(block) - 1) / sizeof(block); }
    /** @brief Allocate a frame using the leading allocator argument.
     *  @param size Frame bytes. @param source Allocator whose resource outlives the task.
     *  @param args Remaining coroutine arguments. */
    template <typename... Args>
    static void* operator new(std::size_t size, const A& source, Args&... args) {
        (void)sizeof...(args);
        if (size > std::numeric_limits<std::size_t>::max() - sizeof(block) - sizeof(A)) throw std::bad_alloc();
        allocator alloc(source);
        const auto offset = slots(size);
        auto* memory = traits::allocate(alloc, offset + slots(sizeof(A)));
        std::construct_at(reinterpret_cast<A*>(memory + offset), source);
        return memory;
    }
    /** @brief Free with the original allocator after frame/argument destruction.
     *  @param memory Allocation base. @param size Original frame bytes. */
    static void operator delete(void* memory, std::size_t size) noexcept {
        auto* base = static_cast<block*>(memory);
        auto* saved = reinterpret_cast<A*>(base + slots(size));
        allocator alloc(*saved);
        std::destroy_at(saved);
        traits::deallocate(alloc, base, slots(size) + slots(sizeof(A)));
    }
};
/** @brief Default tasks keep the compiler's ordinary allocation path unchanged. */
template <> struct frame<void> {};
} // namespace snowy::detail
