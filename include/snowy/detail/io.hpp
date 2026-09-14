/** @file io.hpp
 *  @brief Stable native request state embedded in an awaiting coroutine. */
#pragma once
#include "snowy/loop.hpp"
#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#include <mswsock.h>
#else
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <sys/uio.h>
#endif

namespace snowy::detail {
#ifdef _WIN32
using socket_id = SOCKET;
inline constexpr socket_id invalid_socket = INVALID_SOCKET;
#else
using socket_id = int;
inline constexpr socket_id invalid_socket = -1;
#endif

/** @brief Supported native request kinds. */
enum class opcode { nop, read, write, accept, connect, recv_from, send_to };

/** @brief Single-shot native request; nonmovable while the kernel retains it. */
struct io : op {
#ifdef _WIN32
    /** @brief Native completion address with a portable owner back-pointer. */
    struct packet : OVERLAPPED {
        io* request = nullptr;
    } overlapped{};
    WSABUF buffer{};
    char accept_buffer[2 * (sizeof(sockaddr_storage) + 16)]{};
    DWORD flags = 0;
#else
    iovec vector{};
    msghdr message{};
#endif
    opcode code;
    socket_id fd;
    void* data;
    unsigned size;
    sockaddr_storage address{};
    int address_size = 0;
    socket_id accepted = invalid_socket; ///< Owned until accept() transfers it.
    std::size_t bytes = 0;
    std::stop_token token;
    io* next = nullptr;
    io* prev = nullptr;
    bool canceling = false;
    bool cancel_sent = false;
    bool started = false;
    bool done = false; ///< Linux retains this request until its cancel CQE too.
    bool* busy = nullptr; ///< Socket direction, borrowed until await completion.
    bool reserved = false;

    /** @brief Bind one native request.
     *  @param context Owner loop. @param kind Operation kind.
     *  @param socket Native socket. @param buffer Borrowed buffer.
     *  @param length Buffer bytes. @param stop Optional cancellation token.
     *  @param slot Socket direction to reserve, or nullptr for independent I/O. */
    io(loop& context, opcode kind, socket_id socket = invalid_socket,
       void* buffer = nullptr, unsigned length = 0, std::stop_token stop = {}, bool* slot = nullptr)
        : op(context), code(kind), fd(socket), data(buffer), size(length), token(stop), busy(slot) {}
    /** @brief Release an accepted socket not consumed by the caller. */
    ~io();
    /** @brief Submit after registering cancellation. @param h Suspended coroutine. */
    bool await_suspend(std::coroutine_handle<> h);
    /** @brief Return transferred bytes. @throws std::system_error on I/O failure. */
    std::size_t await_resume() {
        if (reserved) { *busy = false; reserved = false; }
        op::await_resume();
        return bytes;
    }
};

/** @brief Close an owned socket. @param fd Socket or invalid_socket. */
inline void close(socket_id fd) noexcept {
    if (fd == invalid_socket) return;
#ifdef _WIN32
    ::closesocket(fd);
#else
    ::close(fd);
#endif
}
} // namespace snowy::detail
