/** @file kqueue.cpp
 *  @brief Nonblocking socket readiness translated into single-shot completions. */
#include "snowy/detail/io.hpp"
#include <sys/event.h>
#include <cerrno>

namespace snowy {
struct loop::driver { int queue = -1; };

void loop::attach(std::uintptr_t) {}

loop::loop() : driver_(std::make_unique<driver>()) {
    driver_->queue = kqueue();
    if (driver_->queue < 0) throw std::system_error(errno, std::generic_category(), "kqueue");
    struct kevent event;
    EV_SET(&event, 0, EVFILT_USER, EV_ADD | EV_CLEAR, 0, 0, nullptr);
    if (kevent(driver_->queue, &event, 1, nullptr, 0, nullptr) < 0) {
        const int error = errno;
        ::close(driver_->queue);
        throw std::system_error(error, std::generic_category(), "kevent user");
    }
}

loop::~loop() {
    if (running_ || roots_ || io_ || waits_ || holds_ || !messages_.empty()
        || !timers_.empty() || !ready_.empty()) std::terminate();
    ::close(driver_->queue);
}

void loop::wake() noexcept {
    struct kevent event;
    EV_SET(&event, 0, EVFILT_USER, 0, NOTE_TRIGGER, 0, nullptr);
    int r;
    do { r = kevent(driver_->queue, &event, 1, nullptr, 0, nullptr); }
    while (r < 0 && errno == EINTR);
    if (r < 0) std::terminate();
}

namespace {
/** @brief Select readiness direction. @param op Native socket request. */
short filter(const detail::io& op) {
    return op.code == detail::opcode::read || op.code == detail::opcode::accept
        || op.code == detail::opcode::recv_from
        ? EVFILT_READ : EVFILT_WRITE;
}
}

void loop::submit(detail::io& op) {
    ssize_t r = 0;
    do {
        switch (op.code) {
        case detail::opcode::nop: r = 0; break;
        case detail::opcode::file_read:
        case detail::opcode::file_write:
        case detail::opcode::file_sync:
            op.error = std::make_error_code(std::errc::operation_not_supported);
            complete(op);
            return; // Regular file operations use pool workers, not readiness.
        case detail::opcode::read: r = ::recv(op.fd, op.data, op.size, 0); break;
        case detail::opcode::write: r = ::send(op.fd, op.data, op.size, 0); break;
        case detail::opcode::accept: r = ::accept(op.fd, nullptr, nullptr); break;
        case detail::opcode::recv_from:
        case detail::opcode::send_to:
            op.vector = {op.data, op.size};
            op.message = {};
            op.message.msg_name = &op.address;
            op.message.msg_namelen = static_cast<socklen_t>(op.address_size);
            op.message.msg_iov = &op.vector;
            op.message.msg_iovlen = 1;
            r = op.code == detail::opcode::recv_from
                ? ::recvmsg(op.fd, &op.message, 0) : ::sendmsg(op.fd, &op.message, 0);
            if (r >= 0 && op.code == detail::opcode::recv_from) {
                op.address_size = static_cast<int>(op.message.msg_namelen);
                if (op.message.msg_flags & MSG_TRUNC) {
                    op.error = std::make_error_code(std::errc::message_size);
                }
            }
            break;
        case detail::opcode::connect:
            if (!op.started) {
                r = ::connect(op.fd, reinterpret_cast<sockaddr*>(&op.address),
                              static_cast<socklen_t>(op.address_size));
                op.started = true;
            } else {
                int error = 0;
                socklen_t length = sizeof(error);
                r = ::getsockopt(op.fd, SOL_SOCKET, SO_ERROR, &error, &length);
                if (r == 0 && error) { errno = error; r = -1; }
            }
            break;
        }
    } while (r < 0 && errno == EINTR && op.code != detail::opcode::connect);
    if (r >= 0) {
        if (op.code == detail::opcode::accept) op.accepted = static_cast<int>(r);
        else op.bytes = static_cast<std::size_t>(r);
        complete(op);
    } else if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINPROGRESS
               || (errno == EINTR && op.code == detail::opcode::connect)) {
        struct kevent event;
        EV_SET(&event, static_cast<uintptr_t>(op.fd), filter(op), EV_ADD | EV_ONESHOT, 0, 0, &op);
        int result;
        do { result = kevent(driver_->queue, &event, 1, nullptr, 0, nullptr); }
        while (result < 0 && errno == EINTR);
        if (result < 0) {
            op.error = {errno, std::generic_category()};
            complete(op);
        }
    } else {
        op.error = {errno, std::generic_category()};
        complete(op);
    }
}

void loop::cancel(detail::io& op) {
    struct kevent event;
    EV_SET(&event, static_cast<uintptr_t>(op.fd), filter(op), EV_DELETE, 0, 0, nullptr);
    int r;
    do { r = kevent(driver_->queue, &event, 1, nullptr, 0, nullptr); }
    while (r < 0 && errno == EINTR);
    if (r < 0 && errno != ENOENT)
        throw std::system_error(errno, std::generic_category(), "kevent cancel");
    op.cancel_sent = true;
    op.error = std::make_error_code(std::errc::operation_canceled);
    complete(op);
}

void loop::poll(std::chrono::nanoseconds delay) {
    timespec timeout{};
    timespec* wait = nullptr;
    if (delay.count() >= 0) {
        timeout.tv_sec = static_cast<time_t>(delay.count() / 1'000'000'000);
        timeout.tv_nsec = static_cast<long>(delay.count() % 1'000'000'000);
        wait = &timeout;
    }
    struct kevent events[128];
    const int count = kevent(driver_->queue, nullptr, 0, events, 128, wait);
    if (count < 0) {
        if (errno == EINTR) return;
        throw std::system_error(errno, std::generic_category(), "kevent wait");
    }
    for (int i = 0; i < count; ++i) {
        if (events[i].filter == EVFILT_USER) continue;
        auto& op = *static_cast<detail::io*>(events[i].udata);
        if (events[i].flags & EV_ERROR) {
            op.error = {static_cast<int>(events[i].data), std::generic_category()};
            complete(op);
        } else submit(op); // Readiness is not completion; EAGAIN rearms the filter.
    }
}
} // namespace snowy
