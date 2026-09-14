/** @file uring_multi.hpp
 *  @brief Multishot polling, periodic timers and pollable-fd reads; no silent fallback. */
#pragma once
#include "snowy/uring.hpp"

#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#endif
namespace snowy::uring {
/** @brief Observe native readiness repeatedly with one active request.
 *  @param loop Owner. @param fd Borrowed pollable fd. @param events POLL_* mask.
 *  @param fn Owned bool(unsigned) callback; false stops normally. @param token Cancellation.
 *  @details Callback failures drain before rethrowing. The callback should consume readiness;
 *  fd stays open until return. Do not close/reuse it inside the callback. */
template <typename F>
task<> watch(loop& loop, int fd, unsigned events, F fn, std::stop_token token = {}) {
    for (;;) {
        io_uring_sqe sqe{};
        io_uring_prep_poll_multishot(&sqe, fd, events);
        stream request(loop, sqe, token, nullptr, &fn, [](stream& request, int res, unsigned) {
            if (res >= 0 && !request.stopped && !std::invoke(*static_cast<F*>(request.context), static_cast<unsigned>(res)))
                request.stop();
        });
        try { co_await request; }
        catch (const std::system_error& e) {
            if (request.failure) std::rethrow_exception(request.failure);
            if (request.stopped && e.code() == std::errc::operation_canceled) co_return;
            throw;
        }
        if (request.failure) std::rethrow_exception(request.failure);
        if (request.stopped) co_return;
    }
}

/** @brief Deliver native periodic monotonic timeouts (Linux 6.4+, newer liburing).
 *  @param loop Owner. @param interval Positive duration. @param fn Owned bool() callback.
 *  @param count Tick limit; zero runs until cancellation or callback false. @param token Cancellation.
 *  @details False stops normally; external cancellation throws after all CQEs are drained.
 *  This is kernel periodic timing, not a guarantee of callback latency under load. */
template <typename F>
task<> ticks(loop& loop, std::chrono::nanoseconds interval, F fn, unsigned count = 0,
             std::stop_token token = {}) {
    if (interval.count() <= 0) throw std::invalid_argument("nonpositive timer interval");
#ifdef IORING_TIMEOUT_MULTISHOT
    __kernel_timespec time{interval.count() / 1000000000, interval.count() % 1000000000};
    unsigned received = 0;
    struct context { F& fn; unsigned& received; } context{fn, received};
    do {
        io_uring_sqe sqe{};
        io_uring_prep_timeout(&sqe, &time, count ? count - received : 0, IORING_TIMEOUT_MULTISHOT);
        stream request(loop, sqe, token, nullptr, &context, [](stream& request, int res, unsigned) {
            auto& ctx = *static_cast<struct context*>(request.context);
            if (res == -ETIME && !request.stopped) {
                ++ctx.received;
                if (!std::invoke(ctx.fn)) request.stop();
            }
        });
        try { co_await request; }
        catch (const std::system_error& e) {
            if (request.failure) std::rethrow_exception(request.failure);
            if (request.stopped && e.code() == std::errc::operation_canceled) co_return;
            if (e.code().value() != ETIME) throw;
        }
        if (request.failure) std::rethrow_exception(request.failure);
        if (request.stopped) co_return;
    } while (!count || received < count);
#else
    (void)loop; (void)fn; (void)count; (void)token;
    throw std::system_error(std::make_error_code(std::errc::operation_not_supported), "multishot timeout needs newer liburing");
    co_return;
#endif
}

/** @brief Read a pipe/tun/other pollable fd with provided buffers (Linux 6.7+, liburing 2.5+).
 *  @param loop Owner. @param fd Borrowed fd. @param table Buffer pool.
 *  @param fn Owned bool(chunk) callback; false stops normally. @param token Cancellation.
 *  @details Not for regular files. EOF ends the task; ENOBUFS waits for leases and rearms.
 *  fd/table outlive completion, and the caller serializes other reads on this fd. */
template <typename F>
task<> read(loop& loop, int fd, provided& table, F fn, std::stop_token token = {}) {
#if defined(IO_URING_VERSION_MAJOR) && (IO_URING_VERSION_MAJOR > 2 || IO_URING_VERSION_MINOR >= 5)
    io_uring_sqe sqe{};
    io_uring_prep_read_multishot(&sqe, fd, 0, 0, table.group());
    return receive(loop, sqe, table, std::move(fn), token);
#else
    (void)loop; (void)fd; (void)table; (void)fn; (void)token;
    throw std::system_error(std::make_error_code(std::errc::operation_not_supported), "multishot read needs liburing 2.5+");
#endif
}
} // namespace snowy::uring
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic pop
#endif
