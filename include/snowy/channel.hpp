/** @file channel.hpp
 *  @brief Owner-thread FIFO with allocation-free awaiters and direct waiter handoff. */
#pragma once
#include "snowy/event.hpp"

namespace snowy {
/** @brief Bounded FIFO; zero capacity is a rendezvous, not an unbounded queue.
 *  @details All methods require the owner thread. Close drains buffered values.
 *  T must move without throwing. The channel outlives every pending awaiter. */
template <typename T>
class channel {
    static_assert(std::is_nothrow_move_constructible_v<T>);
public:
    /** @brief Allocate fixed storage. @param loop Owner. @param capacity Buffer slots, possibly zero. */
    channel(loop& loop, std::size_t capacity) : loop_(loop), slots_(capacity) { loop.check(); }
    channel(const channel&) = delete;
    channel& operator=(const channel&) = delete;
    /** @brief Reject destruction while waiters still reference the queue. */
    ~channel() { if (first_) std::terminate(); }
private:
    /** @brief Common intrusive node, linked until matched or canceled. */
    struct node : detail::wait {
        channel& queue;
        std::stop_token token;
        std::optional<T> value;
        node* next_ = nullptr;
        node* prev_ = nullptr;
        bool sending, linked = false;
        /** @brief Bind a one-shot operation. @param c Owner. @param send Direction.
         *  @param token Cancellation. @param value Owned outgoing payload or empty receive slot. */
        node(channel& c, bool send, std::stop_token token, std::optional<T> value)
            : detail::wait(c.loop_, true), queue(c), token(token), value(std::move(value)), sending(send) {}
        /** @brief Unlink a canceled but unmatched waiter. */
        ~node() { if (linked) queue.unlink(*this); }
        /** @brief Match immediately or enqueue once. @param h Suspended caller. */
        bool await_suspend(std::coroutine_handle<> h) {
            owner.check();
            if (owner.stopped() || token.stop_requested()) {
                error = std::make_error_code(std::errc::operation_canceled); return false;
            }
            if (sending) {
                if (queue.closed_) { error = std::make_error_code(std::errc::broken_pipe); return false; }
                if (queue.push(*value)) return false;
            } else {
                auto received = queue.pop();
                if (received) { value.emplace(std::move(*received)); return false; }
                if (queue.closed_) return false;
            }
            if (!park(h, token)) return false;
            linked = true;
            prev_ = queue.last_;
            if (prev_) prev_->next_ = this; else queue.first_ = this;
            queue.last_ = this;
            return true;
        }
    };
public:
    /** @brief Coroutine-owned send/receive state; no child coroutine allocation. */
    template <bool Send>
    struct awaiter : node {
        /** @brief Bind operation storage. @param c Queue. @param token Cancellation.
         *  @param value Outgoing payload or empty receive slot. */
        awaiter(channel& c, std::stop_token token, std::optional<T> value = {})
            : node(c, Send, token, std::move(value)) {}
        /** @brief Observe errors and move the received value, or nullopt after close. */
        auto await_resume() {
            detail::op::await_resume();
            if constexpr (!Send) return std::move(this->value);
        }
    };
    /** @brief Send an owned value with backpressure. @param value Payload. @param token Cancellation. */
    [[nodiscard]] awaiter<true> send(T value, std::stop_token token = {}) {
        return {*this, token, std::optional<T>{std::move(value)}};
    }
    /** @brief Receive a value, or nullopt at drained close. @param token Cancellation. */
    [[nodiscard]] awaiter<false> recv(std::stop_token token = {}) { return {*this, token}; }
    /** @brief Try to send without waiting. @param value Moved only on success.
     *  @return False when full, closed or stopped. */
    bool try_send(T& value) { loop_.check(); return !closed_ && !loop_.stopped() && push(value); }
    /** @brief Try to receive; nullopt means empty, stopped or drained close. */
    std::optional<T> try_recv() {
        loop_.check();
        if (loop_.stopped()) return {};
        return pop();
    }
    /** @brief Whether close has been requested. */
    bool closed() const { loop_.check(); return closed_; }
    /** @brief Close idempotently, rejecting senders and completing empty receivers. */
    void close() {
        loop_.check();
        closed_ = true;
        while (first_) {
            auto& waiter = *first_;
            unlink(waiter);
            if (!waiter.active) continue;
            if (waiter.sending) waiter.error = std::make_error_code(std::errc::broken_pipe);
            waiter.notify();
        }
    }
private:
    loop& loop_;
    std::vector<std::optional<T>> slots_;
    std::size_t head_ = 0, size_ = 0;
    bool closed_ = false;
    node* first_ = nullptr;
    node* last_ = nullptr;
    /** @brief Unlink without resuming. @param waiter Linked node. */
    void unlink(node& waiter) noexcept {
        if (waiter.prev_) waiter.prev_->next_ = waiter.next_; else first_ = waiter.next_;
        if (waiter.next_) waiter.next_->prev_ = waiter.prev_; else last_ = waiter.prev_;
        waiter.linked = false;
    }
    /** @brief Match one live opposite waiter, pruning canceled nodes. @param sending Desired direction. */
    node* take(bool sending) {
        while (first_) {
            auto* waiter = first_;
            if (waiter->active && !waiter->canceled.load(std::memory_order_relaxed) && !loop_.stopped()) {
                if (waiter->sending != sending) return nullptr;
                unlink(*waiter);
                return waiter;
            }
            unlink(*waiter);
            if (waiter->active) {
                waiter->error = std::make_error_code(std::errc::operation_canceled);
                waiter->notify();
            }
        }
        return nullptr;
    }
    /** @brief Transfer directly or buffer. @param value Payload, moved only on success. */
    bool push(T& value) {
        if (auto* receiver = take(false)) {
            receiver->value.emplace(std::move(value));
            receiver->notify();
            return true;
        }
        if (size_ == slots_.size()) return false;
        slots_[(head_ + size_) % slots_.size()].emplace(std::move(value));
        ++size_;
        return true;
    }
    /** @brief Receive in FIFO order, refilling the freed slot from one blocked sender. */
    std::optional<T> pop() {
        std::optional<T> value;
        if (size_) {
            value.emplace(std::move(*slots_[head_]));
            slots_[head_].reset();
            head_ = (head_ + 1) % slots_.size();
            --size_;
        }
        if (auto* sender = take(true)) {
            if (value) {
                slots_[(head_ + size_) % slots_.size()].emplace(std::move(*sender->value));
                ++size_;
            } else value.emplace(std::move(*sender->value));
            sender->notify();
        }
        return value;
    }
};
} // namespace snowy
