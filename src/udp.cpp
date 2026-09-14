/** @file udp.cpp
 *  @brief Datagram ownership and compositions over the existing native I/O state. */
#include "snowy/udp.hpp"
#include <algorithm>
#include <climits>

namespace snowy {
namespace {
/** @brief Raise a native socket error. */
[[noreturn]] void error() {
#ifdef _WIN32
    throw std::system_error(WSAGetLastError(), std::system_category());
#else
    throw std::system_error(errno, std::generic_category());
#endif
}
}
detail::socket_id udp::open(int family) {
#ifdef _WIN32
    auto fd = WSASocketW(family, SOCK_DGRAM, IPPROTO_UDP, nullptr, 0,
                        WSA_FLAG_OVERLAPPED | WSA_FLAG_NO_HANDLE_INHERIT);
#else
    auto fd = ::socket(family, SOCK_DGRAM, IPPROTO_UDP);
#endif
    if (fd == detail::invalid_socket) error();
    return fd;
}
udp::udp(loop& loop, endpoint address) : socket_(loop, open(address.address_.ss_family)) {
    if (::bind(socket_.fd_, reinterpret_cast<sockaddr*>(&address.address_), address.size_)) error();
}
task<datagram> udp::recv(std::span<std::byte> buffer, std::stop_token token) {
    socket_.check();
    detail::io op{*socket_.loop_, detail::opcode::recv_from, socket_.fd_, buffer.data(),
        static_cast<unsigned>(std::min<std::size_t>(buffer.size(), INT_MAX)), token, &socket_.reading_};
    op.address_size = sizeof(op.address);
    const auto count = co_await op;
    endpoint peer;
    peer.address_ = op.address;
    peer.size_ = op.address_size;
    co_return datagram{count, peer};
}
task<std::size_t> udp::send(std::span<const std::byte> buffer, endpoint peer, std::stop_token token) {
    socket_.check();
    if (buffer.size() > 65507)
        throw std::system_error(std::make_error_code(std::errc::message_size));
    detail::io op{*socket_.loop_, detail::opcode::send_to, socket_.fd_,
        const_cast<std::byte*>(buffer.data()), static_cast<unsigned>(buffer.size()), token, &socket_.writing_};
    op.address = peer.address_;
    op.address_size = peer.size_;
    const auto count = co_await op;
    if (count != buffer.size()) throw std::system_error(std::make_error_code(std::errc::message_size));
    co_return count;
}
} // namespace snowy
