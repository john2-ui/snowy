/** @file loop.hpp
 *  @brief Single-thread event loop with native wakeups and cancellable timers. */
#pragma once

#include "snowy/detail/intrusive_queue.hpp"
#include "snowy/sync_wait.hpp"
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <stop_token>
#include <system_error>
#include <thread>
#include <vector>

namespace snowy {
class loop;
namespace detail {
struct io;

/** @brief Coroutine-owned ready node; queued once and consumed on its loop. */
struct work : ready_operation {
    std::coroutine_handle<> handle;
};

/** @brief Pending operation; stop callbacks only mark it and wake the owner. */
struct op : work {
    loop& owner;
    std::error_code error;
    std::atomic_bool canceled{false};
    bool active = false; ///< Destroying a pending operation is a contract violation.

    /** @brief Bind to an owner. @param context Loop that outlives this operation. */
    explicit op(loop& context) : owner(context) {}
    /** @brief Verify that native references have been retired. */
    ~op();
    /** @brief Always enter the suspension hook. */
    bool await_ready() const noexcept { return false; }
    /** @brief Observe the terminal error. @throws std::system_error on failure. */
    void await_resume();

    /** @brief Thread-safe callback with no queued raw pointers. */
    struct cancel_fn {
        op* target;
        /** @brief Mark cancellation without touching owner-thread state. */
        void operator()() const noexcept;
    };
    std::optional<std::stop_callback<cancel_fn>> callback;
};
} // namespace detail

/** @brief Owner-thread loop; only post(), stop() and stop tokens are thread-safe.
 *  @details Construct, run, and destroy on the same thread. run() drains spawned
 *  tasks and queued posts, then returns at idle. Keep the loop alive until all
 *  producers have stopped posting. Destroying a loop with live tasks terminates.
 */
class loop {
public:
    using clock = std::chrono::steady_clock;

    /** @brief Create the native queue. @throws std::system_error on OS failure. */
    loop();
    /** @brief Release an idle native queue. */
    ~loop();
    loop(const loop&) = delete;
    loop& operator=(const loop&) = delete;

    /** @brief Queue a root task; run() reports its first unhandled error.
     *  @param input Task consumed on the owner thread. */
    void spawn(task<> input);
    /** @brief Drain all roots and posts; may be called again after normal exit.
     *  @throws Any unhandled task error, after stopping and draining siblings. */
    void run();
    /** @brief Run a root and any children it spawns. @param input Consumed task. */
    void run(task<> input) { spawn(std::move(input)); run(); }
    /** @brief Queue a function from any thread.
     *  @param fn Owned callback, executed on the loop; errors stop the loop. */
    void post(std::function<void()> fn);
    /** @brief Request cancellation of pending and future operations; thread-safe.
     *  @details Permanent for this loop. run() still drains task cleanup. */
    void stop() noexcept;
    /** @brief Verify thread affinity. @throws std::logic_error on a wrong thread. */
    void check() const;

    /** @brief Allocation-free cooperative yield. */
    struct yield : detail::work {
        loop& owner;
        /** @brief Bind a yield. @param context Owner loop. */
        explicit yield(loop& context) : owner(context) {}
        /** @brief Yield even when no other work is ready. */
        bool await_ready() const noexcept { return false; }
        /** @brief Queue the caller. @param h Suspended coroutine. */
        void await_suspend(std::coroutine_handle<> h);
        /** @brief Complete the yield. */
        void await_resume() const noexcept {}
    };
    /** @brief Return a one-shot yield awaiter. */
    [[nodiscard]] yield schedule() { return yield{*this}; }

    /** @brief Coroutine-owned timer, valid until its await completes. */
    struct timer : detail::op {
        clock::time_point due;
        std::stop_token token;
        /** @brief Bind a deadline.
         *  @param context Owner loop. @param deadline Monotonic deadline.
         *  @param stop Optional cancellation token. */
        timer(loop& context, clock::time_point deadline, std::stop_token stop)
            : op(context), due(deadline), token(stop) {}
        /** @brief Arm the timer. @param h Suspended coroutine. */
        bool await_suspend(std::coroutine_handle<> h);
    };
    /** @brief Suspend until a monotonic deadline or cancellation.
     *  @param due Deadline. @param token Optional cancellation token. */
    [[nodiscard]] timer sleep_until(clock::time_point due, std::stop_token token = {}) {
        return timer{*this, due, token};
    }
    /** @brief Suspend for a duration; nonpositive values complete on the next tick.
     *  @param delay Duration. @param token Optional cancellation token. */
    [[nodiscard]] timer sleep(clock::duration delay, std::stop_token token = {});

private:
    friend class socket;
    friend struct detail::op;
    friend struct detail::op::cancel_fn;
    friend struct detail::io;
    struct driver;
    std::unique_ptr<driver> driver_;
    std::thread::id thread_ = std::this_thread::get_id();
    detail::ready_queue ready_;
    std::vector<timer*> timers_;
    std::mutex mutex_;
    std::vector<std::function<void()>> posts_;
    std::atomic_bool stopping_{false};
    std::atomic_bool cancel_{false};
    std::size_t roots_ = 0;
    bool running_ = false;
    std::exception_ptr error_;
    detail::io* io_ = nullptr; ///< Intrusive list of native requests.

    /** @brief Own a root until terminal completion. @param input Consumed task. */
    detail::detached start(task<> input);
    /** @brief Record the first error and cancel remaining work. */
    void fail() noexcept;
    /** @brief Register cancellation. @param op Pending operation. @param token Token. */
    bool arm(detail::op& op, std::stop_token token);
    /** @brief Retire callback before queuing a continuation. @param op Finished operation. */
    void finish(detail::op& op) noexcept;
    /** @brief Signal a native wakeup; callable from other threads. */
    void wake() noexcept;
    /** @brief Collect native events. @param delay Maximum wait; negative means forever. */
    void poll(std::chrono::nanoseconds delay);
    /** @brief Submit native I/O. @param op Stable operation address. */
    void submit(detail::io& op);
    /** @brief Request native cancellation. @param op Pending operation. */
    void cancel(detail::io& op);
    /** @brief Process cancellation and remove expired timers. */
    void expire();
    /** @brief Mark an I/O terminal result. @param op Completed native request. */
    void complete(detail::io& op) noexcept;
    /** @brief Attach a socket once, outside the I/O hot path. @param fd Native socket. */
    void attach(std::uintptr_t fd);
};
} // namespace snowy
