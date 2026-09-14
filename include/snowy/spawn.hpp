/** @file spawn.hpp
 *  @brief Result-bearing spawned tasks with coroutine and blocking joins. */
#pragma once
#include "snowy/futex.hpp"

namespace snowy::detail {
/** @brief Single-consumer result with shared lifetime and two kinds of waiter. */
template <typename T>
struct spawn_state : wait_state<T> {
    std::atomic_bool complete{false};
    futex<bool> signal{complete};
    /** @brief Consume the completed result exactly once. */
    T take() {
        if (this->error) std::rethrow_exception(this->error);
        if constexpr (!std::is_void_v<T>) return std::move(*this->value);
    }
};
/** @brief Own a child and publish its result without escaping errors to loop.run().
 *  @param input Child. @param state Shared result. */
template <typename T, typename A>
task<> publish(task<T, A> input, std::shared_ptr<spawn_state<T>> state) {
    try {
        if constexpr (std::is_void_v<T>) co_await std::move(input);
        else state->value.emplace(co_await std::move(input));
    } catch (...) { state->error = std::current_exception(); }
    {
        std::lock_guard lock(state->mutex);
        state->done = true;
        state->complete.store(true, std::memory_order_release);
        state->ready.notify_one();
    }
    state->signal.notify_all();
}
} // namespace snowy::detail

namespace snowy {
/** @brief Move-only single-consumer task result; transferable between threads.
 *  @details Awaiting/get consumes the handle. Destruction or detach discards the
 *  result, not the running task; errors are then discarded too. Joining drains
 *  through loop.stop(), so children must eventually finish or honor cancellation.
 *  Do not access the same handle concurrently. */
template <typename T>
class join_handle {
    std::shared_ptr<detail::spawn_state<T>> state_;
    /** @brief Validate and transfer the result reference. */
    auto release() {
        if (!state_) throw std::logic_error("empty join handle");
        return std::exchange(state_, {});
    }
public:
    /** @brief Bind shared result storage. @param state Producer-owned state. */
    explicit join_handle(std::shared_ptr<detail::spawn_state<T>> state) : state_(std::move(state)) {}
    join_handle(const join_handle&) = delete;
    join_handle& operator=(const join_handle&) = delete;
    join_handle(join_handle&&) noexcept = default;
    join_handle& operator=(join_handle&&) noexcept = default;
    /** @brief Test whether a result reference is held. */
    explicit operator bool() const noexcept { return bool(state_); }
    /** @brief Test completion without consuming the result. */
    bool ready() const noexcept { return state_ && state_->complete.load(std::memory_order_acquire); }
    /** @brief Discard the result; the spawning loop still owns task cleanup. */
    void detach() noexcept { state_.reset(); }
    /** @brief Embedded noncancelable result waiter; no additional coroutine frame. */
    struct awaiter {
        std::shared_ptr<detail::spawn_state<T>> state;
        typename futex<bool>::awaiter wait;
        /** @brief Bind stable state before constructing the waiter. @param value Result.
         *  @param owner Resumption loop. */
        awaiter(std::shared_ptr<detail::spawn_state<T>> value, loop& owner)
            : state(std::move(value)), wait(state->signal.join(owner, false)) {}
        /** @brief Enter the affinity-checked suspension hook. */
        bool await_ready() const noexcept { return false; }
        /** @brief Wait until publication. @param h Continuation. */
        bool await_suspend(std::coroutine_handle<> h) { return wait.await_suspend(h); }
        /** @brief Consume the result after the publication barrier. */
        T await_resume() { wait.await_resume(); return state->take(); }
    };
    /** @brief Consume and join on an explicit loop. @param owner Resumption loop. */
    [[nodiscard]] awaiter join(loop& owner) && { return {release(), owner}; }
    /** @brief Consume and join on the currently executing loop. */
    [[nodiscard]] awaiter operator co_await() && {
        auto* owner = loop::current();
        if (!owner) throw std::logic_error("join requires a running loop");
        return {release(), *owner};
    }
    /** @brief Consume and block outside event-loop execution; does not drive a loop. */
    T get() && {
        if (loop::current()) throw std::logic_error("blocking join on a loop");
        auto state = release();
        std::unique_lock lock(state->mutex);
        state->ready.wait(lock, [&] { return state->done; });
        return state->take();
    }
};

/** @brief Start a result-bearing task on its owner thread. @param owner Running or future loop.
 *  @param input Consumed task. @return A single-consumer handle; may be moved to another thread. */
template <typename T, typename A>
[[nodiscard]] join_handle<T> spawn(loop& owner, task<T, A> input) {
    owner.check();
    if (!input) throw std::invalid_argument("empty task");
    auto state = std::make_shared<detail::spawn_state<T>>();
    owner.spawn(detail::publish(std::move(input), state));
    return join_handle<T>{std::move(state)};
}
} // namespace snowy
