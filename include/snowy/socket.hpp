/** @file socket.hpp
 *  @brief Move-only TCP sockets, numeric endpoints, and partial I/O awaiters. */
#pragma once
#include "snowy/detail/io.hpp"
#include <span>
#include <string_view>

namespace snowy {
/** @brief Numeric IPv4/IPv6 address and host-order port; no DNS lookup. */
class endpoint {
public:
    /** @brief Parse a numeric address.
     *  @param ip IPv4 or IPv6 literal. @param port Host-order port, zero for ephemeral.
     *  @throws std::invalid_argument if ip is invalid or contains a scope suffix. */
    endpoint(std::string_view ip = "0.0.0.0", std::uint16_t port = 0);
    /** @brief Return the host-order port. */
    std::uint16_t port() const noexcept;
private:
    friend class socket;
    sockaddr_storage address_{};
    int size_ = sizeof(sockaddr_in);
};

/** @brief TCP socket bound to a loop; one read and one write may run concurrently.
 *  @details Socket, loop, and buffers must outlive their operations. Moving,
 *  closing, or destroying a socket with pending I/O is a contract violation.
 *  Methods require the owner thread; cancellation tokens may come from any thread.
 */
class socket {
public:
    /** @brief Move an idle socket. @param other Socket that becomes empty.
     *  @throws std::logic_error when other has active I/O or the thread is wrong. */
    socket(socket&& other);
    socket& operator=(socket&&) = delete;
    socket(const socket&) = delete;
    socket& operator=(const socket&) = delete;
    /** @brief Close an idle owned socket. */
    ~socket();

    /** @brief Bind a listening TCP socket.
     *  @param loop Owner. @param address Bind endpoint. @param backlog Listen queue size.
     *  @throws std::system_error on socket, bind, or listen failure. */
    static socket listen(loop& loop, endpoint address, int backlog = 128);
    /** @brief Connect a new socket, closing it on cancellation or error.
     *  @param loop Owner. @param address Remote endpoint. @param token Cancellation token.
     *  @return Connected socket. @throws std::system_error on connection failure. */
    static task<socket> connect(loop& loop, endpoint address, std::stop_token token = {});
    /** @brief Accept one peer.
     *  @param token Cancellation token. @return Connected socket.
     *  @throws std::system_error on accept failure. */
    task<socket> accept(std::stop_token token = {});
    /** @brief Read up to buffer.size() bytes; zero means EOF for nonempty buffers.
     *  @param buffer Borrowed writable bytes. @param token Cancellation token.
     *  @return Awaiter yielding the byte count; errors throw std::system_error. */
    [[nodiscard]] detail::io read(std::span<std::byte> buffer, std::stop_token token = {});
    /** @brief Write up to buffer.size() bytes; partial writes are normal.
     *  @param buffer Borrowed immutable bytes. @param token Cancellation token.
     *  @return Awaiter yielding the byte count; errors throw std::system_error. */
    [[nodiscard]] detail::io write(std::span<const std::byte> buffer, std::stop_token token = {});
    /** @brief Half-close the send direction after writes finish.
     *  @throws std::system_error on shutdown failure. */
    void shutdown();
    /** @brief Set TCP_NODELAY. @param enabled Whether to disable Nagle's algorithm. */
    void nodelay(bool enabled = true);
    /** @brief Return the bound local endpoint, including an assigned ephemeral port. */
    endpoint local() const;
    /** @brief Expose a borrowed native socket; callers must preserve ownership. */
    detail::socket_id native_handle() const noexcept { return fd_; }

private:
    loop* loop_;
    detail::socket_id fd_;
    bool reading_ = false;
    bool writing_ = false;
    /** @brief Adopt and configure a socket, closing on failure.
     *  @param loop Owner. @param fd Transferred socket. */
    socket(loop& loop, detail::socket_id fd);
    /** @brief Reject empty sockets and calls from a different thread. */
    void check() const;
    /** @brief Open a TCP socket. @param family AF_INET or AF_INET6. */
    static detail::socket_id open(int family);
};

/** @brief Write every byte or throw; partial progress is not rolled back.
 *  @param socket Borrowed socket. @param buffer Borrowed bytes. @param token Cancellation token.
 *  @throws std::system_error on failure or no progress. */
task<> write_all(socket& socket, std::span<const std::byte> buffer, std::stop_token token = {});
} // namespace snowy
