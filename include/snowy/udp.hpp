/** @file udp.hpp
 *  @brief Datagram sockets preserving message boundaries and source endpoints. */
#pragma once
#include "snowy/socket.hpp"

namespace snowy {
/** @brief A received datagram; zero bytes is a valid message, not EOF. */
struct datagram {
    std::size_t size;
    endpoint peer;
};
/** @brief Owner-thread UDP socket; one send and one receive may run concurrently.
 *  @details Socket and buffers must outlive their operations. Oversized incoming
 *  messages are consumed and reported as message_size, never silently truncated. */
class udp {
public:
    /** @brief Bind a UDP endpoint. @param loop Owner. @param address Local bind address. */
    udp(loop& loop, endpoint address = {});
    /** @brief Move an idle socket. @param other Source becoming empty. */
    udp(udp&& other) : socket_(std::move(other.socket_)) {}
    udp(const udp&) = delete;
    udp& operator=(const udp&) = delete;
    /** @brief Return the assigned local port and address. */
    endpoint local() const { return socket_.local(); }
    /** @brief Receive one datagram. @param buffer Borrowed destination.
     *  @param token Optional cancellation token. @return Length and sender endpoint. */
    task<datagram> recv(std::span<std::byte> buffer, std::stop_token token = {});
    /** @brief Send one whole datagram, including zero-length messages.
     *  @param buffer Borrowed payload, at most 65507 bytes. @param peer Destination.
     *  @param token Optional stop token. @return Sent bytes. */
    task<std::size_t> send(std::span<const std::byte> buffer, endpoint peer,
                           std::stop_token token = {});
private:
    socket socket_;
    /** @brief Open a datagram socket. @param family Address family. */
    static detail::socket_id open(int family);
};
} // namespace snowy
