/** @file uring.cpp
 *  @brief io_uring single-shot backend with batched completion retirement. */
#include "snowy/detail/io.hpp"
#include <liburing.h>
#include <sys/eventfd.h>
#include <poll.h>
#include <cerrno>
#include <cstdint>

namespace snowy {
struct loop::driver {
    io_uring ring{};
    int event = -1;
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
    }
};

loop::loop() : driver_(std::make_unique<driver>()) {
    // SINGLE_ISSUER arrived after Linux 5.15; retry without it on older kernels.
    io_uring_params params{};
    params.flags = IORING_SETUP_SINGLE_ISSUER;
    int result = io_uring_queue_init_params(256, &driver_->ring, &params);
    if (result == -EINVAL) result = io_uring_queue_init(256, &driver_->ring, 0);
    if (result < 0) throw std::system_error(-result, std::generic_category(), "io_uring_setup");
    driver_->event = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (driver_->event < 0) {
        const int error = errno;
        io_uring_queue_exit(&driver_->ring);
        throw std::system_error(error, std::generic_category(), "eventfd");
    }
    driver_->watch();
}

loop::~loop() {
    if (running_ || roots_ || io_ || waits_ || !timers_.empty() || !ready_.empty()) std::terminate();
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
    auto* sqe = driver_->sqe();
    switch (op.code) {
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
    io_uring_cqe* first = nullptr;
    __kernel_timespec timeout{};
    __kernel_timespec* wait = nullptr;
    if (delay.count() >= 0) {
        timeout.tv_sec = delay.count() / 1'000'000'000;
        timeout.tv_nsec = delay.count() % 1'000'000'000;
        wait = &timeout;
    }
    int r = io_uring_submit_and_wait_timeout(&driver_->ring, &first, 1, wait, nullptr);
    if (r < 0 && r != -ETIME && r != -EINTR)
        throw std::system_error(-r, std::generic_category(), "io_uring wait");

    // Retire the entire snapshot before user code can free or reuse requests.
    io_uring_cqe* batch[128];
    const unsigned count = io_uring_peek_batch_cqe(&driver_->ring, batch, 128);
    bool wake_seen = false;
    for (unsigned i = 0; i < count; ++i) {
        auto* cqe = batch[i];
        const auto tag = static_cast<std::uintptr_t>(io_uring_cqe_get_data64(cqe));
        if (!tag) { wake_seen = true; continue; }
        auto& op = *reinterpret_cast<detail::io*>(tag & ~std::uintptr_t{1});
        if (tag & 1) {
            op.canceling = false;
            if (op.done) complete(op);
        } else {
            op.done = true;
            if (cqe->res < 0) op.error = {-cqe->res, std::generic_category()};
            else if (op.code == detail::opcode::accept) op.accepted = cqe->res;
            else op.bytes = static_cast<std::size_t>(cqe->res);
            if (op.code == detail::opcode::recv_from && cqe->res >= 0) {
                op.address_size = static_cast<int>(op.message.msg_namelen);
                if (op.message.msg_flags & MSG_TRUNC)
                    op.error = std::make_error_code(std::errc::message_size);
            }
            if (!op.canceling) complete(op);
        }
    }
    io_uring_cq_advance(&driver_->ring, count);
    if (wake_seen) {
        std::uint64_t value;
        ssize_t result;
        do { result = ::read(driver_->event, &value, sizeof(value)); }
        while (result < 0 && errno == EINTR);
        if (result < 0 && errno != EAGAIN)
            throw std::system_error(errno, std::generic_category(), "eventfd read");
        driver_->watch();
    }
}
} // namespace snowy
