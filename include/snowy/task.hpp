/**
 * @file task.hpp
 * @brief Defines Snowy's lazy, move-only C++20 coroutine task.
 */

#pragma once

#include <concepts>
#include <coroutine>
#include <exception>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace snowy {

template <typename T>
class task;

namespace detail {

/**
 * @brief State shared by every task promise.
 */
class task_promise_base {
public:
    /**
     * @brief Keeps a task lazy until it is awaited or scheduled.
     * @return An awaiter that suspends coroutine entry.
     */
    std::suspend_always initial_suspend() const noexcept { return {}; }

    /**
     * @brief Captures an exception for rethrow by the awaiting coroutine.
     */
    void unhandled_exception() noexcept { exception_ = std::current_exception(); }

    /**
     * @brief Transfers execution directly to the awaiting coroutine.
     */
    struct final_awaiter {
        /** @brief Final suspension must always run the transfer hook. */
        bool await_ready() const noexcept { return false; }

        /**
         * @brief Selects the continuation to resume.
         * @tparam Promise Concrete task promise type.
         * @param handle Handle of the task reaching final suspension.
         * @return Awaiting coroutine, or a no-op coroutine for a root task.
         */
        template <typename Promise>
        std::coroutine_handle<> await_suspend(
            std::coroutine_handle<Promise> handle) const noexcept {
            return handle.promise().continuation_;
        }

        /** @brief Completes the final-suspend protocol. */
        void await_resume() const noexcept {}
    };

    /**
     * @brief Returns the symmetric-transfer final awaiter.
     * @return Final awaiter bound by await_suspend to this promise.
     */
    final_awaiter final_suspend() const noexcept { return {}; }

    /**
     * @brief Records the coroutine that is awaiting this task.
     * @param continuation Coroutine to resume when this task finishes.
     */
    void set_continuation(std::coroutine_handle<> continuation) noexcept {
        continuation_ = continuation;
    }

protected:
    /** @brief Rethrows the exception captured by unhandled_exception, if any. */
    void rethrow_if_failed() const {
        if (exception_) {
            std::rethrow_exception(exception_);
        }
    }

private:
    std::coroutine_handle<> continuation_ = std::noop_coroutine();
    std::exception_ptr exception_;
};

template <typename T>
class task_promise final : public task_promise_base {
    static_assert(!std::is_void_v<T>);
    static_assert(!std::is_reference_v<T>, "task<T&> is not supported");

public:
    /**
     * @brief Creates the task object that owns this coroutine frame.
     * @return A lazy task bound to this promise.
     */
    task<T> get_return_object() noexcept;

    /**
     * @brief Stores the value returned by the coroutine.
     * @tparam U Source value type.
     * @param value Value used to construct T.
     */
    template <typename U>
        requires std::constructible_from<T, U&&>
    void return_value(U&& value) noexcept(std::is_nothrow_constructible_v<T, U&&>) {
        value_.emplace(std::forward<U>(value));
    }

    /**
     * @brief Moves the completed result out of the promise.
     * @return The coroutine result.
     * @throws Any exception raised by the coroutine or by moving T.
     */
    T take_result() {
        rethrow_if_failed();
        return std::move(*value_);
    }

private:
    std::optional<T> value_;
};

template <>
class task_promise<void> final : public task_promise_base {
public:
    /**
     * @brief Creates the void task that owns this coroutine frame.
     * @return A lazy task bound to this promise.
     */
    task<void> get_return_object() noexcept;

    /** @brief Marks successful completion of a void coroutine. */
    void return_void() const noexcept {}

    /** @brief Rethrows an exception captured while running the coroutine. */
    void take_result() const { rethrow_if_failed(); }
};

} // namespace detail

/**
 * @brief A lazy, uniquely owned coroutine result.
 * @tparam T Result type; references are deliberately unsupported.
 * @details Awaiting an rvalue task transfers execution directly into it. The
 * awaiting coroutine resumes through symmetric transfer at final suspension.
 */
template <typename T = void>
class [[nodiscard]] task {
public:
    using promise_type = detail::task_promise<T>;
    using handle_type = std::coroutine_handle<promise_type>;

    /** @brief Construct an empty task; awaiting it throws logic_error. */
    task() noexcept = default;

    /**
     * @brief Transfers ownership from another task.
     * @param other Task that becomes empty.
     */
    task(task&& other) noexcept : handle_(std::exchange(other.handle_, {})) {}

    /**
     * @brief Replaces the owned, unconsumed coroutine.
     * @param other Task that becomes empty.
     * @return This task.
     */
    task& operator=(task&& other) noexcept {
        if (this != &other) {
            if (handle_) {
                handle_.destroy();
            }
            handle_ = std::exchange(other.handle_, {});
        }
        return *this;
    }

    task(const task&) = delete;
    task& operator=(const task&) = delete;

    /** @brief Destroys an unconsumed coroutine frame. */
    ~task() {
        if (handle_) {
            handle_.destroy();
        }
    }

    /**
     * @brief Reports whether this object owns a coroutine.
     * @return True when the task can be awaited or scheduled.
     */
    [[nodiscard]] explicit operator bool() const noexcept {
        return static_cast<bool>(handle_);
    }

    /**
     * @brief Awaiter that owns the child frame until result extraction.
     */
    class awaiter {
    public:
        /** @brief Own a child frame. @param handle Transferred frame or null. */
        explicit awaiter(handle_type handle) noexcept : handle_(handle) {}
        awaiter(const awaiter&) = delete;
        awaiter& operator=(const awaiter&) = delete;

        /** @brief Destroys the completed child frame. */
        ~awaiter() {
            if (handle_) {
                handle_.destroy();
            }
        }

        /**
         * @brief Detects an empty task before await_suspend dereferences it.
         * @return True only for an empty task, which fails in await_resume.
         */
        bool await_ready() const noexcept { return !handle_; }

        /**
         * @brief Starts the child and registers its continuation.
         * @param continuation Awaiting coroutine.
         * @return Child coroutine selected for symmetric transfer.
         */
        std::coroutine_handle<> await_suspend(
            std::coroutine_handle<> continuation) noexcept {
            handle_.promise().set_continuation(continuation);
            return handle_;
        }

        /**
         * @brief Extracts the child result after completion.
         * @return T for value tasks; void for task<void>.
         * @throws Any exception raised by the child coroutine.
         */
        decltype(auto) await_resume() {
            if (!handle_) [[unlikely]] {
                throw std::logic_error("cannot await an empty task");
            }
            return handle_.promise().take_result();
        }

    private:
        handle_type handle_;
    };

    /**
     * @brief Transfers this task into an await expression.
     * @return An awaiter that owns the coroutine frame.
     */
    awaiter operator co_await() && noexcept {
        return awaiter{std::exchange(handle_, {})};
    }

    awaiter operator co_await() & = delete;

private:
    friend promise_type;

    /** @brief Own a new frame. @param handle Initial-suspended coroutine. */
    explicit task(handle_type handle) noexcept : handle_(handle) {}

    handle_type handle_{};
};

namespace detail {

template <typename T>
task<T> task_promise<T>::get_return_object() noexcept {
    return task<T>{std::coroutine_handle<task_promise>::from_promise(*this)};
}

inline task<void> task_promise<void>::get_return_object() noexcept {
    return task<void>{
        std::coroutine_handle<task_promise<void>>::from_promise(*this)};
}

} // namespace detail

} // namespace snowy
