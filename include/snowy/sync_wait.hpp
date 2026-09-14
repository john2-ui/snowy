/** @file sync_wait.hpp
 *  @brief Blocking bridge for tasks completed locally or on another thread. */
#pragma once

#include "snowy/task.hpp"
#include <condition_variable>
#include <mutex>
#include <variant>

namespace snowy::detail {

/** @brief Eager bridge that releases its own frame at completion. */
struct detached {
    struct promise_type {
        /** @brief Return the stateless bridge token. */
        detached get_return_object() const noexcept { return {}; }
        /** @brief Start immediately. */
        std::suspend_never initial_suspend() const noexcept { return {}; }
        /** @brief Release the completed frame. */
        std::suspend_never final_suspend() const noexcept { return {}; }
        /** @brief Finish without a value. */
        void return_void() const noexcept {}
        /** @brief Bridges must handle errors before leaving their body. */
        void unhandled_exception() const noexcept { std::terminate(); }
    };
};

/** @brief Caller-owned result and completion barrier. */
template <typename T>
struct wait_state {
    std::mutex mutex;
    std::condition_variable ready;
    bool done = false; ///< Protected by mutex, including notification.
    std::optional<std::conditional_t<std::is_void_v<T>, std::monostate, T>> value;
    std::exception_ptr error;
};

/** @brief Publish a task result before releasing the waiting thread.
 *  @param input Consumed task.
 *  @param state Caller-owned state; do not access it after unlocking. */
template <typename T, typename A>
detached wait_task(task<T, A> input, wait_state<T>& state) {
    try {
        if constexpr (std::is_void_v<T>) {
            co_await std::move(input);
        } else {
            state.value.emplace(co_await std::move(input));
        }
    } catch (...) {
        state.error = std::current_exception();
    }
    // Keep notification inside the lifetime barrier for the condition variable.
    std::lock_guard lock(state.mutex);
    state.done = true;
    state.ready.notify_one();
}
} // namespace snowy::detail

namespace snowy {
/** @brief Block until a task completes; does not drive an event loop.
 *  @param input Consumed task; external events require another running thread.
 *  @return The result, or void for a void task.
 *  @throws Any error from the task or result construction. */
template <typename T, typename A>
T sync_wait(task<T, A> input) {
    detail::wait_state<T> state;
    detail::wait_task(std::move(input), state);
    std::unique_lock lock(state.mutex);
    state.ready.wait(lock, [&state] { return state.done; });
    if (state.error) std::rethrow_exception(state.error);
    if constexpr (!std::is_void_v<T>) return std::move(*state.value);
}
} // namespace snowy
