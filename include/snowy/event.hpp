/** @file event.hpp
 *  @brief Owner-thread manual-reset event and cancellable suspension nodes. */
#pragma once
#include "snowy/loop.hpp"

namespace snowy::detail {
/** @brief Coroutine-owned wait registered with its loop until notification. */
struct wait : op {
    wait* next = nullptr;
    wait* prev = nullptr;
    bool interruptible;
    void (*interrupt)(wait&) noexcept = nullptr; ///< Optional synchronized cancellation hook.
    /** @brief Bind a wait. @param loop Owner. @param cancelable Honor stop requests. */
    wait(loop& loop, bool cancelable) : op(loop), interruptible(cancelable) {}
    /** @brief Park once. @param h Continuation. @param token Optional cancellation. */
    bool park(std::coroutine_handle<> h, std::stop_token token);
    /** @brief Queue completion on the owner thread; ignore already retired waits. */
    void notify() noexcept;
};
}

namespace snowy {
/** @brief Manual-reset broadcast event; all methods require the owner thread.
 *  @details Must outlive every waiter, including canceled but not yet resumed ones. */
class event {
public:
    /** @brief Bind an initially unset event. @param loop Owner loop. */
    explicit event(loop& loop) : loop_(loop) {}
    event(const event&) = delete;
    event& operator=(const event&) = delete;
    /** @brief Reject destruction while waiters still reference the event. */
    ~event() { if (head_) std::terminate(); }
    /** @brief Set the event and queue all waiters, without inline resumption. */
    void set() {
        loop_.check();
        set_ = true;
        for (auto* w = head_; w; w = w->next_) w->notify();
    }
    /** @brief Reset the event for future waiters. */
    void reset() { loop_.check(); set_ = false; }
    /** @brief One-shot suspension node, embedded in the awaiting frame. */
    struct awaiter : detail::wait {
        event& event_;
        std::stop_token token_;
        awaiter* next_ = nullptr;
        awaiter* prev_ = nullptr;
        bool linked_ = false;
        /** @brief Bind an awaiter. @param e Event. @param token Stop token.
         *  @param cancelable False only for cleanup that must finish before return. */
        awaiter(event& e, std::stop_token token, bool cancelable)
            : detail::wait(e.loop_, cancelable), event_(e), token_(token) {}
        /** @brief Unlink after completion or canceled suspension. */
        ~awaiter() {
            if (!linked_) return;
            if (prev_) prev_->next_ = next_; else event_.head_ = next_;
            if (next_) next_->prev_ = prev_;
        }
        /** @brief Register before inspecting state. @param h Continuation. */
        bool await_suspend(std::coroutine_handle<> h) {
            if (!park(h, token_)) return false;
            next_ = event_.head_;
            if (next_) next_->prev_ = this;
            event_.head_ = this;
            linked_ = true;
            if (event_.set_) notify();
            return true;
        }
    };
    /** @brief Wait for set or cancellation. @param token Optional stop token. */
    [[nodiscard]] awaiter wait(std::stop_token token = {}) { return {*this, token, true}; }
    /** @brief Wait despite loop.stop(); use for draining externally owned work.
     *  @details The producer must eventually signal, even on errors. */
    [[nodiscard]] awaiter join() { return {*this, {}, false}; }
private:
    loop& loop_;
    bool set_ = false;
    awaiter* head_ = nullptr;
};
} // namespace snowy
