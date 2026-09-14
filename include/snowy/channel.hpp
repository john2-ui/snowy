/** @file channel.hpp
 *  @brief Bounded owner-thread coroutine channel with backpressure and close. */
#pragma once
#include "snowy/event.hpp"

namespace snowy {
/** @brief Bounded FIFO; T must move without throwing to preserve received messages.
 *  @details All methods require the owner thread. Close drains buffered values,
 *  rejects senders, and returns nullopt to receivers once empty. Not cross-thread. */
template <typename T>
class channel {
    static_assert(std::is_nothrow_move_constructible_v<T>);
public:
    /** @brief Allocate fixed capacity. @param loop Owner. @param capacity Positive slots. */
    channel(loop& loop, std::size_t capacity)
        : loop_(loop), slots_(capacity), readable_(loop), writable_(loop) {
        loop.check();
        if (!capacity) throw std::invalid_argument("zero channel capacity");
    }
    /** @brief Send one owned value, waiting for space. @param value Moved message.
     *  @param token Cancellation token. @throws std::system_error if closed/canceled. */
    task<> send(T value, std::stop_token token = {}) {
        loop_.check();
        for (;;) {
            if (token.stop_requested() || loop_.stopped())
                throw std::system_error(std::make_error_code(std::errc::operation_canceled));
            if (closed_) throw std::system_error(std::make_error_code(std::errc::broken_pipe));
            if (size_ < slots_.size()) break;
            writable_.reset();
            co_await writable_.wait(token);
        }
        slots_[(head_ + size_) % slots_.size()].emplace(std::move(value));
        ++size_;
        readable_.set();
    }
    /** @brief Receive one value or nullopt at drained close. @param token Stop token. */
    task<std::optional<T>> recv(std::stop_token token = {}) {
        loop_.check();
        while (!size_) {
            if (token.stop_requested() || loop_.stopped())
                throw std::system_error(std::make_error_code(std::errc::operation_canceled));
            if (closed_) co_return std::nullopt;
            readable_.reset();
            co_await readable_.wait(token);
        }
        if (token.stop_requested() || loop_.stopped())
            throw std::system_error(std::make_error_code(std::errc::operation_canceled));
        auto result = std::move(slots_[head_]);
        slots_[head_].reset();
        head_ = (head_ + 1) % slots_.size();
        --size_;
        writable_.set();
        co_return result;
    }
    /** @brief Close idempotently; wake blocked senders and receivers. */
    void close() { loop_.check(); closed_ = true; readable_.set(); writable_.set(); }
private:
    loop& loop_;
    std::vector<std::optional<T>> slots_;
    std::size_t head_ = 0, size_ = 0;
    bool closed_ = false;
    // ponytail: broadcast wakeups keep the queue small; use per-waiter handoff
    // only if many-producer/consumer profiling shows a thundering herd.
    event readable_, writable_;
};
} // namespace snowy
