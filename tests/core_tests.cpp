/**
 * @file core_tests.cpp
 * @brief Exercises task ownership, transfer, errors, and the ready FIFO.
 */

#include "snowy/detail/intrusive_queue.hpp"
#include "snowy/snowy.hpp"

#include <cstdlib>
#include <stdexcept>
#include <string>
#include <utility>
#include <thread>
#include <memory>

namespace {

/**
 * @brief Terminates the test executable when a condition is false.
 * @param condition Condition required by the current test.
 */
void check(bool condition) {
    if (!condition) {
        std::abort();
    }
}

/** @brief Produces a value without external scheduling. */
snowy::task<int> answer() { co_return 42; }

/** @brief Verifies nested tasks resume through their continuation. */
snowy::task<std::string> nested() {
    const auto value = co_await answer();
    co_return std::to_string(value);
}

/** @brief Throws from inside a task to test await-side propagation. */
snowy::task<void> failing() {
    co_await std::suspend_never{};
    throw std::runtime_error("expected failure");
}

/** @brief Completes a void task successfully. */
snowy::task<void> completes() { co_return; }

/** @brief Transfer execution to a caller-owned thread. */
struct on_thread {
    std::thread& worker; ///< Joined by the test after sync_wait returns.
    /** @brief Always transfer. */
    bool await_ready() const noexcept { return false; }
    /** @brief Publish the handle only after capturing all awaiter state.
     *  @param h Suspended coroutine; no awaiter access after thread launch. */
    void await_suspend(std::coroutine_handle<> h) const {
        auto& output = worker;
        output = std::thread([h] { h.resume(); });
    }
    /** @brief Complete the transfer. */
    void await_resume() const noexcept {}
};

/** @brief Return a move-only value on another thread.
 *  @param worker Thread owned and joined by the caller. */
snowy::task<std::unique_ptr<int>> remote(std::thread& worker) {
    co_await on_thread{worker};
    co_return std::make_unique<int>(42);
}

/** @brief Throw a value-move exception to check frame cleanup. */
struct throwing {
    throwing() = default;
    /** @brief Reject every move. */
    throwing(throwing&&) { throw std::runtime_error("move"); }
};

/** @brief Exercise an exception during result construction. */
snowy::task<throwing> bad_move() { co_return throwing{}; }

/** @brief Checks the observable task behavior with standard assertions. */
void test_task() {
    auto pending = answer();
    check(static_cast<bool>(pending));
    auto moved = std::move(pending);
    check(!static_cast<bool>(pending));
    check(snowy::sync_wait(std::move(moved)) == 42);
    check(snowy::sync_wait(nested()) == "42");
    snowy::sync_wait(completes());

    bool caught = false;
    try {
        snowy::sync_wait(failing());
    } catch (const std::runtime_error& error) {
        caught = std::string{error.what()} == "expected failure";
    }
    check(caught);

    caught = false;
    try {
        snowy::sync_wait(std::move(pending));
    } catch (const std::logic_error& error) {
        caught = std::string{error.what()} == "cannot await an empty task";
    }
    check(caught);
}

/** @brief Check cross-thread publication and exceptional result cleanup. */
void test_wait() {
    for (int i = 0; i < 200; ++i) {
        std::thread worker;
        check(*snowy::sync_wait(remote(worker)) == 42);
        worker.join();
    }
    bool caught = false;
    try { (void)snowy::sync_wait(bad_move()); }
    catch (const std::runtime_error&) { caught = true; }
    check(caught);
}

/** @brief Checks FIFO order and node reuse after removal. */
void test_ready_queue() {
    snowy::detail::ready_operation first;
    snowy::detail::ready_operation second;
    snowy::detail::ready_queue queue;

    check(queue.empty());
    queue.push(first);
    queue.push(second);
    check(queue.pop() == &first);
    check(queue.pop() == &second);
    check(queue.pop() == nullptr);
    check(queue.empty());

    queue.push(first);
    check(queue.pop() == &first);
}

} // namespace

/**
 * @brief Runs the dependency-free core checks.
 * @return Zero when every assertion holds.
 */
int main() {
    test_task();
    test_wait();
    test_ready_queue();
}
