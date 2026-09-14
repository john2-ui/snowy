/** @file loop.cpp
 *  @brief Owner-thread scheduling and cancellation, independent of the OS. */
#include "snowy/detail/io.hpp"
#include <algorithm>
#include <limits>

namespace snowy {
namespace {
/** @brief Order the earliest timer first. @param a Left timer. @param b Right timer. */
bool later(const loop::timer* a, const loop::timer* b) { return a->due > b->due; }
}

detail::op::~op() {
    if (active) std::terminate();
}

void detail::op::await_resume() {
    if (error) throw std::system_error(error);
}

void detail::op::cancel_fn::operator()() const noexcept {
    target->canceled.store(true, std::memory_order_relaxed);
    target->owner.cancel_.store(true, std::memory_order_release);
    target->owner.wake();
}

void loop::check() const {
    if (thread_ != std::this_thread::get_id())
        throw std::logic_error("wrong loop thread");
}

void loop::yield::await_suspend(std::coroutine_handle<> h) {
    owner.check();
    handle = h;
    owner.ready_.push(*this);
}

loop::timer loop::sleep(clock::duration delay, std::stop_token token) {
    const auto now = clock::now();
    const auto room = clock::time_point::max() - now;
    return timer{*this, delay <= clock::duration::zero() ? now
        : delay >= room ? clock::time_point::max() : now + delay, token};
}

bool loop::arm(detail::op& op, std::stop_token token) {
    check();
    if (stopping_.load(std::memory_order_relaxed) || token.stop_requested()) {
        op.error = std::make_error_code(std::errc::operation_canceled);
        return false;
    }
    if (token.stop_possible()) op.callback.emplace(token, detail::op::cancel_fn{&op});
    op.active = true;
    return true;
}

bool loop::timer::await_suspend(std::coroutine_handle<> h) {
    owner.check();
    // Reserve before exposing the operation to cancellation callbacks.
    owner.timers_.push_back(this);
    try {
        if (!owner.arm(*this, token)) {
            owner.timers_.pop_back();
            return false;
        }
    } catch (...) {
        owner.timers_.pop_back();
        throw;
    }
    handle = h;
    std::push_heap(owner.timers_.begin(), owner.timers_.end(), later);
    return true;
}

void loop::finish(detail::op& op) noexcept {
    op.callback.reset(); // Wait for an in-flight stop callback before frame reuse.
    op.active = false;
    ready_.push(op);
}

void loop::stop() noexcept {
    stopping_.store(true, std::memory_order_relaxed);
    cancel_.store(true, std::memory_order_release);
    wake();
}

void loop::post(std::function<void()> fn) {
    if (!fn) throw std::invalid_argument("empty post");
    std::lock_guard lock(mutex_);
    posts_.push_back(std::move(fn));
    if (posts_.size() == 1) wake();
}

void loop::fail() noexcept {
    if (!error_) error_ = std::current_exception();
    stop();
}

detail::detached loop::start(task<> input) {
    ++roots_;
    try {
        co_await schedule();
        co_await std::move(input);
    } catch (...) { fail(); }
    --roots_;
}

void loop::spawn(task<> input) {
    check();
    if (!input) throw std::invalid_argument("empty task");
    start(std::move(input));
}

detail::io::~io() { detail::close(accepted); }

bool detail::io::await_suspend(std::coroutine_handle<> h) {
    if (!owner.arm(*this, token)) return false;
    handle = h;
    next = owner.io_;
    if (next) next->prev = this;
    owner.io_ = this;
    try { owner.submit(*this); }
    catch (...) {
        if (next) next->prev = nullptr;
        owner.io_ = next;
        callback.reset();
        active = false;
        throw;
    }
    return true;
}

void loop::complete(detail::io& op) noexcept {
    if (op.prev) op.prev->next = op.next;
    else io_ = op.next;
    if (op.next) op.next->prev = op.prev;
    finish(op);
}

void loop::expire() {
    if (cancel_.exchange(false, std::memory_order_acquire)) {
        const bool all = stopping_.load(std::memory_order_relaxed);
        // ponytail: cancellation scans pending work once per batch; use indexed
        // cancellation queues only if profiling shows this scan dominates.
        auto end = std::remove_if(timers_.begin(), timers_.end(), [&](timer* t) {
            if (!all && !t->canceled.load(std::memory_order_relaxed)) return false;
            t->error = std::make_error_code(std::errc::operation_canceled);
            finish(*t);
            return true;
        });
        timers_.erase(end, timers_.end());
        std::make_heap(timers_.begin(), timers_.end(), later);
        for (auto* op = io_; op;) {
            auto* next = op->next;
            if (!op->cancel_sent && (all || op->canceled.load(std::memory_order_relaxed)))
                cancel(*op);
            op = next;
        }
    }
    const auto now = clock::now();
    while (!timers_.empty() && timers_.front()->due <= now) {
        auto* t = timers_.front();
        std::pop_heap(timers_.begin(), timers_.end(), later);
        timers_.pop_back();
        finish(*t);
    }
}

void loop::run() {
    check();
    if (running_) throw std::logic_error("nested loop.run");
    running_ = true;
    try {
        std::vector<std::function<void()>> batch;
        for (;;) {
            {
                std::lock_guard lock(mutex_);
                batch.swap(posts_);
            }
            for (auto& fn : batch) {
                try { fn(); } catch (...) { fail(); }
            }
            batch.clear();
            expire();
            // Bound cooperative work between polls so I/O and timers progress.
            for (unsigned i = 0; i < 64; ++i) {
                auto* node = ready_.pop();
                if (!node) break;
                static_cast<detail::work*>(node)->handle.resume();
            }
            bool posted;
            {
                std::lock_guard lock(mutex_);
                posted = !posts_.empty();
            }
            if (!roots_ && !io_ && timers_.empty() && ready_.empty() && !posted) break;
            auto delay = std::chrono::nanoseconds{-1};
            if (!ready_.empty() || posted) delay = std::chrono::nanoseconds{0};
            else if (!timers_.empty()) {
                delay = std::max(std::chrono::nanoseconds{0},
                    std::chrono::duration_cast<std::chrono::nanoseconds>(timers_.front()->due - clock::now()));
            }
            poll(delay);
        }
    } catch (...) {
        running_ = false;
        throw;
    }
    running_ = false;
    if (auto error = std::exchange(error_, {})) std::rethrow_exception(error);
}
} // namespace snowy
