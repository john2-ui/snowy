/** @file socket.cpp
 *  @brief TCP handle ownership and small compositions over native requests. */
#include "snowy/socket.hpp"
#include <algorithm>
#include <climits>
#include <string>
#ifndef _WIN32
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#endif

namespace snowy {
namespace {
/** @brief Throw the last socket error. @param what Failed system call. */
[[noreturn]] void socket_error(const char* what) {
#ifdef _WIN32
    throw std::system_error(WSAGetLastError(), std::system_category(), what);
#else
    throw std::system_error(errno, std::generic_category(), what);
#endif
}

/** @brief Set a portable integer socket option.
 *  @param fd Socket. @param level Protocol level. @param name Option.
 *  @param value Integer value. */
void option(detail::socket_id fd, int level, int name, int value) {
    if (setsockopt(fd, level, name, reinterpret_cast<const char*>(&value), sizeof(value)))
        socket_error("setsockopt");
}
}

endpoint::endpoint(std::string_view ip, std::uint16_t port) {
    if (ip.find('\0') != std::string_view::npos) throw std::invalid_argument("invalid address");
    const std::string text(ip);
    auto* v4 = reinterpret_cast<sockaddr_in*>(&address_);
    if (inet_pton(AF_INET, text.c_str(), &v4->sin_addr) == 1) {
        v4->sin_family = AF_INET;
        v4->sin_port = htons(port);
    } else {
        auto* v6 = reinterpret_cast<sockaddr_in6*>(&address_);
        if (inet_pton(AF_INET6, text.c_str(), &v6->sin6_addr) != 1)
            throw std::invalid_argument("invalid numeric address");
        v6->sin6_family = AF_INET6;
        v6->sin6_port = htons(port);
        size_ = sizeof(sockaddr_in6);
    }
}

std::uint16_t endpoint::port() const noexcept {
    return address_.ss_family == AF_INET
        ? ntohs(reinterpret_cast<const sockaddr_in*>(&address_)->sin_port)
        : ntohs(reinterpret_cast<const sockaddr_in6*>(&address_)->sin6_port);
}

detail::socket_id socket::open(int family) {
#ifdef _WIN32
    auto fd = WSASocketW(family, SOCK_STREAM, IPPROTO_TCP, nullptr, 0,
                        WSA_FLAG_OVERLAPPED | WSA_FLAG_NO_HANDLE_INHERIT);
#else
    auto fd = ::socket(family, SOCK_STREAM, IPPROTO_TCP);
#endif
    if (fd == detail::invalid_socket) socket_error("socket");
    return fd;
}

socket::socket(loop& loop, detail::socket_id fd) : loop_(&loop), fd_(fd) {
    try {
        check();
#ifndef _WIN32
        if (fcntl(fd, F_SETFD, FD_CLOEXEC) < 0) socket_error("fcntl CLOEXEC");
#ifdef __APPLE__
        if (fcntl(fd, F_SETFL, O_NONBLOCK) < 0) socket_error("fcntl NONBLOCK");
        option(fd, SOL_SOCKET, SO_NOSIGPIPE, 1);
#endif
#endif
        loop.attach(static_cast<std::uintptr_t>(fd));
    } catch (...) { detail::close(fd_); throw; }
}

socket::socket(socket&& other) : loop_(other.loop_), fd_(detail::invalid_socket) {
    other.check();
    if (other.reading_ || other.writing_) throw std::logic_error("move of busy socket");
    fd_ = std::exchange(other.fd_, detail::invalid_socket);
}

socket::~socket() {
    if (reading_ || writing_) std::terminate();
    detail::close(fd_);
}

void socket::check() const {
    loop_->check();
    if (fd_ == detail::invalid_socket) throw std::logic_error("empty socket");
}

socket socket::listen(loop& loop, endpoint address, int backlog) {
    loop.check();
    if (backlog <= 0) throw std::invalid_argument("invalid backlog");
    socket result(loop, open(address.address_.ss_family));
#ifdef _WIN32
    option(result.fd_, SOL_SOCKET, SO_EXCLUSIVEADDRUSE, 1);
#else
    option(result.fd_, SOL_SOCKET, SO_REUSEADDR, 1);
#endif
    if (::bind(result.fd_, reinterpret_cast<sockaddr*>(&address.address_), address.size_))
        socket_error("bind");
    if (::listen(result.fd_, backlog)) socket_error("listen");
    return result;
}

task<socket> socket::connect(loop& loop, endpoint address, std::stop_token token) {
    loop.check();
    socket result(loop, open(address.address_.ss_family));
    detail::io op{loop, detail::opcode::connect, result.fd_, nullptr, 0, token};
    op.address = address.address_;
    op.address_size = address.size_;
    co_await op;
    co_return result;
}

task<socket> socket::accept(std::stop_token token) {
    check();
    detail::io op{*loop_, detail::opcode::accept, fd_, nullptr, 0, token, &reading_};
    co_await op;
    co_return socket{*loop_, std::exchange(op.accepted, detail::invalid_socket)};
}

detail::io socket::read(std::span<std::byte> buffer, std::stop_token token) {
    check();
    return {*loop_, detail::opcode::read, fd_, buffer.data(),
            static_cast<unsigned>(std::min<std::size_t>(buffer.size(), INT_MAX)), token, &reading_};
}

detail::io socket::write(std::span<const std::byte> buffer, std::stop_token token) {
    check();
    return {*loop_, detail::opcode::write, fd_, const_cast<std::byte*>(buffer.data()),
            static_cast<unsigned>(std::min<std::size_t>(buffer.size(), INT_MAX)), token, &writing_};
}

void socket::shutdown() {
    check();
    if (writing_) throw std::logic_error("shutdown during write");
#ifdef _WIN32
    if (::shutdown(fd_, SD_SEND)) socket_error("shutdown");
#else
    if (::shutdown(fd_, SHUT_WR)) socket_error("shutdown");
#endif
}

void socket::nodelay(bool enabled) {
    check();
    option(fd_, IPPROTO_TCP, TCP_NODELAY, enabled ? 1 : 0);
}

endpoint socket::local() const {
    check();
    endpoint result;
#ifdef _WIN32
    int size = sizeof(result.address_);
#else
    socklen_t size = sizeof(result.address_);
#endif
    if (getsockname(fd_, reinterpret_cast<sockaddr*>(&result.address_), &size))
        socket_error("getsockname");
    result.size_ = static_cast<int>(size);
    return result;
}

task<> write_all(socket& socket, std::span<const std::byte> buffer, std::stop_token token) {
    while (!buffer.empty()) {
        const auto count = co_await socket.write(buffer, token);
        if (!count) throw std::system_error(std::make_error_code(std::errc::broken_pipe));
        buffer = buffer.subspan(count);
    }
}
} // namespace snowy
