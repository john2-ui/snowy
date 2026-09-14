/** @file futex.hpp
 *  @brief Coroutine atomic-value wait/notify without blocking event-loop threads. */
#pragma once
#include "snowy/detail/wait_queue.hpp"

namespace snowy {
/** @brief Thread-safe atomic wait queue, not a wrapper around a blocking OS futex.
 *  @details The atomic and this queue outlive all awaiters. Store the new value
 *  before notify. A notification may complete a waiter even if the value is
 *  unchanged: consumers recheck their predicate, as with a condition variable. */
template <std::integral T>
class futex {
    std::atomic<T>& value_;
    detail::wait_queue queue_;
public:
    /** @brief Bind externally owned atomic storage. @param value Observed atomic. */
    explicit futex(std::atomic<T>& value) : value_(value) {}
    /** @brief Embedded one-shot atomic waiter. */
    struct awaiter : detail::remote_wait {
        futex& source;
        T old;
        /** @brief Bind a wait. @param f Queue. @param owner Resumption loop.
         *  @param old Expected value. @param token Cancellation. @param cancelable Honor stop. */
        awaiter(futex& f, loop& owner, T old, std::stop_token token, bool cancelable = true)
            : remote_wait(f.queue_, owner, token), source(f), old(old) { interruptible = cancelable; }
        /** @brief Atomically check and enqueue relative to notifications. @param h Continuation. */
        bool await_suspend(std::coroutine_handle<> h) {
            owner.check();
            if (interruptible && canceled_now()) return false;
            std::lock_guard lock(queue.mutex);
            if (source.value_.load(std::memory_order_acquire) != old) return false;
            return enqueue(h);
        }
    };
    /** @brief Wait while the atomic equals old. @param owner Resumption loop.
     *  @param old Expected value. @param token Optional cancellation. */
    [[nodiscard]] awaiter wait(loop& owner, T old, std::stop_token token = {}) {
        return {*this, owner, old, token};
    }
    /** @brief Wait despite loop.stop() for mandatory cleanup. @param owner Resumption loop.
     *  @param old Expected value. @details Producer must eventually change and notify. */
    [[nodiscard]] awaiter join(loop& owner, T old) {
        return {*this, owner, old, {}, false};
    }
    /** @brief Wake one live waiter on its own loop; callable from any thread. */
    void notify_one() noexcept {
        std::lock_guard lock(queue_.mutex);
        if (auto* w = detail::remote_wait::front(queue_)) w->deliver();
    }
    /** @brief Wake every waiter on its own loop; callable from any thread. */
    void notify_all() noexcept {
        std::lock_guard lock(queue_.mutex);
        while (auto* w = detail::remote_wait::front(queue_)) w->deliver();
    }
};
} // namespace snowy
