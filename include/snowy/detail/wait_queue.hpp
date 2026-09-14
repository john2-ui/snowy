/** @file wait_queue.hpp
 *  @brief Synchronized intrusive waiters with owner-loop completion delivery. */
#pragma once
#include "snowy/event.hpp"

namespace snowy::detail {
struct remote_wait;
/** @brief Shared FIFO; callers hold mutex while inspecting or changing links. */
struct wait_queue {
    std::mutex mutex;
    remote_wait* first = nullptr;
    remote_wait* last = nullptr;
    std::size_t users = 0; ///< Includes delivered but not yet destroyed awaiters.
    /** @brief Reject destruction while an awaiter still references this queue. */
    ~wait_queue() { if (users) std::terminate(); }
};

/** @brief Coroutine-owned waiter; only its owner loop retires cancellation state. */
struct remote_wait : wait, message {
    wait_queue& queue;
    std::stop_token token;
    remote_wait* before = nullptr;
    remote_wait* after = nullptr;
    bool linked = false;
    /** @brief Bind stable storage. @param q Shared queue. @param context Owner loop.
     *  @param stop Optional cancellation. */
    remote_wait(wait_queue& q, loop& context, std::stop_token stop)
        : wait(context, true), queue(q), token(stop) {
        std::lock_guard lock(queue.mutex);
        ++queue.users;
        run = [](message& msg) noexcept { static_cast<remote_wait&>(msg).notify(); };
        interrupt = [](wait& base) noexcept {
            auto& self = static_cast<remote_wait&>(base);
            std::lock_guard lock(self.queue.mutex);
            // A removed waiter already has a delivery in flight. Do not free it.
            if (!self.linked) return;
            self.unlink();
            self.error = std::make_error_code(std::errc::operation_canceled);
            self.notify();
        };
    }
    /** @brief Release the queue reference after completion, never while suspended. */
    ~remote_wait() {
        std::lock_guard lock(queue.mutex);
        if (linked || active) std::terminate();
        --queue.users;
    }
    /** @brief Check immediate cancellation on the owner. */
    bool canceled_now() {
        owner.check();
        if (!owner.stopped() && !token.stop_requested()) return false;
        error = std::make_error_code(std::errc::operation_canceled);
        return true;
    }
    /** @brief Park and append while holding queue.mutex. @param h Continuation. */
    bool enqueue(std::coroutine_handle<> h) {
        if (!park(h, token)) return false;
        before = queue.last;
        if (before) before->after = this; else queue.first = this;
        queue.last = this;
        linked = true;
        return true;
    }
    /** @brief Remove a linked node while holding queue.mutex. */
    void unlink() noexcept {
        if (before) before->after = after; else queue.first = after;
        if (after) after->before = before; else queue.last = before;
        linked = false;
    }
    /** @brief Publish terminal state under queue.mutex; do not access afterward.
     *  @param failure Terminal error, if any. */
    void deliver(std::error_code failure = {}) noexcept {
        unlink();
        error = failure;
        message::send(owner);
    }
    /** @brief Prune canceled heads under q.mutex; return the first live waiter.
     *  @param q Shared FIFO. */
    static remote_wait* front(wait_queue& q) noexcept {
        while (q.first && q.first->interruptible
               && (q.first->canceled.load(std::memory_order_relaxed) || q.first->owner.stopped()))
            q.first->deliver(std::make_error_code(std::errc::operation_canceled));
        return q.first;
    }
};
} // namespace snowy::detail
