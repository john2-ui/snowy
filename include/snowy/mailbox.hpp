/** @file mailbox.hpp
 *  @brief Thread-safe bounded MPMC channel with allocation-free suspension. */
#pragma once
#include "snowy/detail/wait_queue.hpp"

namespace snowy {
/** @brief FIFO shared across loops; each operation resumes on its specified loop.
 *  @details Zero capacity provides rendezvous. Close drains buffered values.
 *  T must move without throwing. This object outlives every awaiter. Use channel
 *  for same-loop traffic to avoid synchronization and cross-thread delivery. */
template <typename T>
class mailbox {
    static_assert(std::is_nothrow_move_constructible_v<T>);
    detail::wait_queue queue_;
    std::vector<std::optional<T>> slots_;
    std::size_t head_ = 0, size_ = 0;
    bool closed_ = false;
    /** @brief Shared send/receive node. */
    struct node : detail::remote_wait {
        mailbox& box;
        std::optional<T> value;
        bool sending;
        /** @brief Bind an operation. @param b Mailbox. @param owner Resumption loop.
         *  @param send Direction. @param token Cancellation. @param v Owned payload. */
        node(mailbox& b, loop& owner, bool send, std::stop_token token, std::optional<T> v)
            : remote_wait(b.queue_, owner, token), box(b), value(std::move(v)), sending(send) {}
        /** @brief Match or append atomically. @param h Continuation. */
        bool await_suspend(std::coroutine_handle<> h) {
            if (canceled_now()) return false;
            std::lock_guard lock(queue.mutex);
            if (sending) {
                if (box.closed_) { error = std::make_error_code(std::errc::broken_pipe); return false; }
                if (box.push(*value)) return false;
            } else {
                auto v = box.pop();
                if (v) { value.emplace(std::move(*v)); return false; }
                if (box.closed_) return false;
            }
            return enqueue(h);
        }
    };
    /** @brief Find an opposite waiter under the lock. @param sending Desired direction. */
    node* front(bool sending) noexcept {
        auto* w = static_cast<node*>(detail::remote_wait::front(queue_));
        return w && w->sending == sending ? w : nullptr;
    }
    /** @brief Transfer or buffer under the lock. @param value Moved only on success. */
    bool push(T& value) {
        if (auto* w = front(false)) {
            w->value.emplace(std::move(value));
            w->deliver();
            return true;
        }
        if (size_ == slots_.size()) return false;
        slots_[(head_ + size_) % slots_.size()].emplace(std::move(value));
        ++size_;
        return true;
    }
    /** @brief Receive under the lock and refill one newly freed slot. */
    std::optional<T> pop() {
        std::optional<T> result;
        if (size_) {
            result.emplace(std::move(*slots_[head_]));
            slots_[head_].reset();
            head_ = (head_ + 1) % slots_.size();
            --size_;
        }
        if (auto* w = front(true)) {
            if (result) {
                slots_[(head_ + size_) % slots_.size()].emplace(std::move(*w->value));
                ++size_;
            } else result.emplace(std::move(*w->value));
            w->deliver();
        }
        return result;
    }
public:
    /** @brief Allocate the bounded buffer. @param capacity Slots, possibly zero. */
    explicit mailbox(std::size_t capacity) : slots_(capacity) {}
    /** @brief One-shot operation embedded in the caller's frame. */
    template <bool Send>
    struct awaiter : node {
        /** @brief Bind operation state. @param box Queue. @param owner Resumption loop.
         *  @param token Cancellation. @param value Outgoing payload or empty slot. */
        awaiter(mailbox& box, loop& owner, std::stop_token token, std::optional<T> value = {})
            : node(box, owner, Send, token, std::move(value)) {}
        /** @brief Observe errors; receive returns nullopt after drained close. */
        auto await_resume() {
            detail::op::await_resume();
            if constexpr (!Send) return std::move(this->value);
        }
    };
    /** @brief Send with backpressure. @param owner Resumption loop. @param value Payload.
     *  @param token Optional cancellation. */
    [[nodiscard]] awaiter<true> send(loop& owner, T value, std::stop_token token = {}) {
        return {*this, owner, token, std::optional<T>{std::move(value)}};
    }
    /** @brief Receive or observe drained close. @param owner Resumption loop.
     *  @param token Optional cancellation. */
    [[nodiscard]] awaiter<false> recv(loop& owner, std::stop_token token = {}) { return {*this, owner, token}; }
    /** @brief Try sending from any thread. @param value Moved only on success. */
    bool try_send(T& value) {
        std::lock_guard lock(queue_.mutex);
        return !closed_ && push(value);
    }
    /** @brief Try receiving from any thread; nullopt means empty or drained. */
    std::optional<T> try_recv() { std::lock_guard lock(queue_.mutex); return pop(); }
    /** @brief Close from any thread, rejecting senders and waking receivers. */
    void close() noexcept {
        std::lock_guard lock(queue_.mutex);
        closed_ = true;
        while (auto* w = static_cast<node*>(detail::remote_wait::front(queue_)))
            w->deliver(w->sending ? std::make_error_code(std::errc::broken_pipe) : std::error_code{});
    }
};
} // namespace snowy
