/** @file uring.cpp
 *  @brief io_uring single-shot backend with batched completion retirement. */
#include "snowy/uring.hpp"
#include <sys/eventfd.h>
#include <poll.h>
#include <cerrno>
#include <cstdint>

namespace snowy {
struct loop::driver {
    io_uring ring{};
    int event = -1;
    bool iopoll = false;
    bool watching = false, watch_waking = false;
    unsigned drains = 0;
    /** @brief Allocate a submission slot, submitting a full batch if necessary. */
    io_uring_sqe* sqe() {
        auto* entry = io_uring_get_sqe(&ring);
        while (!entry) {
            const int n = io_uring_submit(&ring);
            if (n < 0 && n != -EINTR)
                throw std::system_error(-n, std::generic_category(), "io_uring_submit");
            entry = io_uring_get_sqe(&ring);
        }
        return entry;
    }
    /** @brief Arm the persistent wake source as a single-shot poll. */
    void watch() {
        auto* entry = sqe();
        io_uring_prep_poll_add(entry, event, POLLIN);
        io_uring_sqe_set_data64(entry, 0);
        watching = true;
    }
};

loop::loop() : loop(uring::options{}) {}

loop::loop(const uring::options& config) : driver_(std::make_unique<driver>()) {
    constexpr unsigned allowed = IORING_SETUP_IOPOLL | IORING_SETUP_SQPOLL | IORING_SETUP_SQ_AFF |
        IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_COOP_TASKRUN | IORING_SETUP_DEFER_TASKRUN |
        IORING_SETUP_TASKRUN_FLAG | IORING_SETUP_SUBMIT_ALL;
    if (!config.entries || config.entries > 32768 || !config.budget || (config.flags & ~allowed) ||
        (config.cq_entries && config.cq_entries < config.entries) ||
        ((config.flags & IORING_SETUP_SQ_AFF) && !(config.flags & IORING_SETUP_SQPOLL)) ||
        ((config.flags & IORING_SETUP_SQPOLL) && (config.flags &
            (IORING_SETUP_COOP_TASKRUN | IORING_SETUP_DEFER_TASKRUN | IORING_SETUP_TASKRUN_FLAG))))
        throw std::invalid_argument("invalid uring options");
    budget_ = config.budget;
    driver_->iopoll = config.flags & IORING_SETUP_IOPOLL;
    // SINGLE_ISSUER arrived after Linux 5.15; retry without it on older kernels.
    io_uring_params params{};
    params.flags = config.flags | IORING_SETUP_SINGLE_ISSUER;
    params.cq_entries = config.cq_entries;
    if (config.cq_entries) params.flags |= IORING_SETUP_CQSIZE;
    params.sq_thread_idle = config.idle_ms;
    params.sq_thread_cpu = config.cpu;
    int result = io_uring_queue_init_params(config.entries, &driver_->ring, &params);
    if (result == -EINVAL && !(config.flags & (IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN))) {
        params.flags &= ~IORING_SETUP_SINGLE_ISSUER;
        result = io_uring_queue_init_params(config.entries, &driver_->ring, &params);
    }
    if (result < 0) throw std::system_error(-result, std::generic_category(), "io_uring_setup");
    driver_->event = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (driver_->event < 0) {
        const int error = errno;
        io_uring_queue_exit(&driver_->ring);
        throw std::system_error(error, std::generic_category(), "eventfd");
    }
    if (!driver_->iopoll) driver_->watch();
}

io_uring& uring::access::ring(loop& loop) { loop.check(); return loop.driver_->ring; }
void uring::access::reserve(loop& loop, unsigned count, bool drain) {
    auto& ring = access::ring(loop);
    if (count > ring.sq.ring_entries) throw std::invalid_argument("batch exceeds ring capacity");
    while (io_uring_sq_space_left(&ring) < count) {
        const int n = io_uring_submit(&ring);
        if (n < 0 && n != -EINTR) throw std::system_error(-n, std::generic_category());
    }
    if (drain && loop.driver_->watching && !loop.driver_->watch_waking) {
        // A preceding linked DRAIN may force the next poll through io-wq.
        // Cancellation can then race its installation; a level-triggered wake
        // remains observable even before that poll has reached the kernel.
        loop.wake();
        loop.driver_->watch_waking = true;
    }
}
void uring::access::start(detail::io& request) {
    auto& loop = request.owner;
    request.active = true;
    request.next = loop.io_;
    if (request.next) request.next->prev = &request;
    loop.io_ = &request;
    loop.submit(request);
}

loop::~loop() {
    if (running_ || roots_ || io_ || waits_ || holds_ || !messages_.empty()
        || !timers_.empty() || !ready_.empty()) std::terminate();
    io_uring_queue_exit(&driver_->ring);
    ::close(driver_->event);
}

void loop::wake() noexcept {
    const std::uint64_t value = 1;
    ssize_t result;
    do { result = ::write(driver_->event, &value, sizeof(value)); }
    while (result < 0 && errno == EINTR);
    if (result < 0 && errno != EAGAIN) std::terminate();
}

void loop::submit(detail::io& op) {
    if (driver_->iopoll && op.code != detail::opcode::file_read &&
        op.code != detail::opcode::file_write && op.code != detail::opcode::native)
        throw std::system_error(std::make_error_code(std::errc::operation_not_supported), "IOPOLL is storage-only");
    io_uring_sqe native{};
    if (op.prepare) op.prepare(op, native);
    if (native.flags & IOSQE_IO_DRAIN) {
        if (driver_->watching && !driver_->watch_waking) {
            uring::access::reserve(*this, 1, true);
        }
        op.draining = true;
        ++driver_->drains;
    }
    auto* sqe = driver_->sqe();
    switch (op.code) {
    case detail::opcode::native: *sqe = native; break;
    case detail::opcode::nop: io_uring_prep_nop(sqe); break;
    case detail::opcode::file_read: io_uring_prep_read(sqe, op.fd, op.data, op.size, op.offset); break;
    case detail::opcode::file_write: io_uring_prep_write(sqe, op.fd, op.data, op.size, op.offset); break;
    case detail::opcode::file_sync: io_uring_prep_fsync(sqe, op.fd, 0); break;
    case detail::opcode::read:
        io_uring_prep_recv(sqe, op.fd, op.data, op.size, 0); break;
    case detail::opcode::write:
        io_uring_prep_send(sqe, op.fd, op.data, op.size, MSG_NOSIGNAL); break;
    case detail::opcode::accept:
        io_uring_prep_accept(sqe, op.fd, nullptr, nullptr, SOCK_CLOEXEC); break;
    case detail::opcode::connect:
        io_uring_prep_connect(sqe, op.fd, reinterpret_cast<sockaddr*>(&op.address),
                             static_cast<socklen_t>(op.address_size)); break;
    case detail::opcode::recv_from:
    case detail::opcode::send_to:
        op.vector = {op.data, op.size};
        op.message.msg_name = &op.address;
        op.message.msg_namelen = static_cast<socklen_t>(op.address_size);
        op.message.msg_iov = &op.vector;
        op.message.msg_iovlen = 1;
        if (op.code == detail::opcode::recv_from) io_uring_prep_recvmsg(sqe, op.fd, &op.message, 0);
        else io_uring_prep_sendmsg(sqe, op.fd, &op.message, MSG_NOSIGNAL);
        break;
    }
    io_uring_sqe_set_data(sqe, &op);
}

void loop::attach(std::uintptr_t) {}

void loop::cancel(detail::io& op) {
    auto* sqe = driver_->sqe();
    io_uring_prep_cancel(sqe, &op, 0);
    static_assert(alignof(detail::io) >= 2);
    io_uring_sqe_set_data64(sqe, reinterpret_cast<std::uintptr_t>(&op) | 1);
    op.canceling = true;
    op.cancel_sent = true;
}

void loop::poll(std::chrono::nanoseconds delay) {
    // While DRAIN suppresses the internal poll, keep external stop/post latency bounded.
    if (driver_->drains && (delay.count() < 0 || delay > std::chrono::milliseconds{1}))
        delay = std::chrono::milliseconds{1};
    io_uring_cqe* first = nullptr;
    __kernel_timespec timeout{};
    __kernel_timespec* wait = nullptr;
    if (delay.count() >= 0) {
        timeout.tv_sec = delay.count() / 1'000'000'000;
        timeout.tv_nsec = delay.count() % 1'000'000'000;
        wait = &timeout;
    }
    int r;
    if (driver_->iopoll) {
        // Never put eventfd polling or timeout SQEs on a storage-only ring.
        // Keep pending storage polls nonblocking so cancellation/posts can run.
        r = io_uring_submit(&driver_->ring);
        if (r >= 0) r = io_uring_get_events(&driver_->ring);
        if (!io_) {
            pollfd event{driver_->event, POLLIN, 0};
            timespec limit{};
            if (wait) { limit.tv_sec = wait->tv_sec; limit.tv_nsec = wait->tv_nsec; }
            if (::ppoll(&event, 1, wait ? &limit : nullptr, nullptr) < 0 && errno != EINTR)
                throw std::system_error(errno, std::generic_category(), "ppoll");
        }
        std::uint64_t value;
        while (::read(driver_->event, &value, sizeof(value)) < 0 && errno == EINTR) {}
    } else r = io_uring_submit_and_wait_timeout(&driver_->ring, &first, 1, wait, nullptr);
    if (r < 0 && r != -ETIME && r != -EINTR)
        throw std::system_error(-r, std::generic_category(), "io_uring wait");

    // Retire the entire snapshot before user code can free or reuse requests.
    io_uring_cqe* batch[128];
    const unsigned count = io_uring_peek_batch_cqe(&driver_->ring, batch, 128);
    bool wake_seen = false;
    auto retire = [&](detail::io& op) {
        if (op.draining) --driver_->drains;
        complete(op);
    };
    for (unsigned i = 0; i < count; ++i) {
        auto* cqe = batch[i];
        const auto tag = static_cast<std::uintptr_t>(io_uring_cqe_get_data64(cqe));
        if (!tag) { wake_seen = true; continue; }
        auto& op = *reinterpret_cast<detail::io*>(tag & ~std::uintptr_t{1});
        if (tag & 1) {
            op.canceling = false;
            if (op.done) retire(op);
        } else {
            op.done = !(cqe->flags & IORING_CQE_F_MORE);
            if (op.result) op.result(op, cqe->res, cqe->flags);
            else if (cqe->res < 0) op.error = {-cqe->res, std::generic_category()};
            else if (op.code == detail::opcode::accept) op.accepted = cqe->res;
            else op.bytes = static_cast<std::size_t>(cqe->res);
            if (op.code == detail::opcode::recv_from && cqe->res >= 0) {
                op.address_size = static_cast<int>(op.message.msg_namelen);
                if (op.message.msg_flags & MSG_TRUNC)
                    op.error = std::make_error_code(std::errc::message_size);
            }
            if (op.done && !op.canceling) retire(op);
        }
    }
    io_uring_cq_advance(&driver_->ring, count);
    if (wake_seen) {
        driver_->watching = false;
        driver_->watch_waking = false;
        std::uint64_t value;
        ssize_t result;
        do { result = ::read(driver_->event, &value, sizeof(value)); }
        while (result < 0 && errno == EINTR);
        if (result < 0 && errno != EAGAIN)
            throw std::system_error(errno, std::generic_category(), "eventfd read");
    }
    if (!driver_->iopoll && !driver_->watching && !driver_->drains)
        driver_->watch();
}
} // namespace snowy
