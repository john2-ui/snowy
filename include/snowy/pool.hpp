/** @file pool.hpp
 *  @brief Fixed worker threads for CPU/blocking jobs, returning to the caller loop. */
#pragma once
#include "snowy/event.hpp"
#include <algorithm>
#include <deque>

namespace snowy {
/** @brief FIFO worker pool; functions run off-loop and must eventually return.
 *  @details run() resumes on its owner loop. Cancellation never destroys an
 *  in-flight job: completion is drained first. Pool and loop must outlive jobs. */
class pool {
public:
    /** @brief Start workers. @param threads Positive worker count. */
    explicit pool(unsigned threads = std::max(1u, std::thread::hardware_concurrency()));
    /** @brief Drain queued jobs and join workers; never destroy on a worker thread. */
    ~pool();
    pool(const pool&) = delete;
    pool& operator=(const pool&) = delete;

    /** @brief Run an owned function off-thread and return its result on the loop.
     *  @param loop Calling loop. @param fn Owned callable, possibly move-only.
     *  @param token Cancellation token; running side effects are not rolled back.
     *  @throws The callable's exception, or operation_canceled after cleanup. */
    template <typename F>
    task<std::invoke_result_t<F&>> run(loop& loop, F fn, std::stop_token token = {}) {
        using T = std::invoke_result_t<F&>;
        static_assert(!std::is_reference_v<T>, "pool results must own their value");
        loop.check();
        if (token.stop_requested() || loop.stopped())
            throw std::system_error(std::make_error_code(std::errc::operation_canceled));
        event done(loop);
        std::optional<std::conditional_t<std::is_void_v<T>, std::monostate, T>> value;
        std::exception_ptr error;
        enqueue([&] {
            try {
                if (!token.stop_requested() && !loop.stopped()) {
                    if constexpr (std::is_void_v<T>) std::invoke(fn);
                    else value.emplace(std::invoke(fn));
                }
            } catch (...) { error = std::current_exception(); }
            // Publication through post's mutex is the result lifetime barrier.
            // Nothing may access the coroutine-owned state after this call.
            loop.post([&done] { done.set(); });
        });
        co_await done.join();
        if (token.stop_requested() || loop.stopped())
            throw std::system_error(std::make_error_code(std::errc::operation_canceled));
        if (error) std::rethrow_exception(error);
        if constexpr (!std::is_void_v<T>) co_return std::move(*value);
    }
private:
    std::mutex mutex_;
    std::condition_variable ready_;
    std::deque<std::function<void()>> jobs_;
    std::vector<std::thread> threads_;
    bool stopping_ = false;
    /** @brief Queue owned work. @param fn Function that always publishes completion. */
    void enqueue(std::function<void()> fn);
    /** @brief Consume until stopped and drained. */
    void work() noexcept;
    /** @brief Wake workers and join after the queue drains. */
    void close() noexcept;
};
} // namespace snowy
