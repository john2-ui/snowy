/** @file pool.cpp
 *  @brief Worker lifetime and FIFO job dispatch. */
#include "snowy/pool.hpp"

namespace snowy {
pool::pool(unsigned threads) {
    if (!threads) throw std::invalid_argument("zero pool threads");
    try {
        threads_.reserve(threads);
        for (unsigned i = 0; i < threads; ++i) threads_.emplace_back([this] { work(); });
    } catch (...) { close(); throw; }
}
pool::~pool() { close(); }
void pool::close() noexcept {
    {
        std::lock_guard lock(mutex_);
        stopping_ = true;
    }
    ready_.notify_all();
    for (auto& t : threads_) t.join();
}
void pool::enqueue(std::function<void()> fn) {
    {
        std::lock_guard lock(mutex_);
        if (stopping_) throw std::logic_error("closed pool");
        jobs_.push_back(std::move(fn));
    }
    ready_.notify_one();
}
void pool::work() noexcept {
    for (;;) {
        std::function<void()> job;
        {
            std::unique_lock lock(mutex_);
            ready_.wait(lock, [this] { return stopping_ || !jobs_.empty(); });
            if (jobs_.empty()) return;
            job = std::move(jobs_.front());
            jobs_.pop_front();
        }
        job();
    }
}
} // namespace snowy
