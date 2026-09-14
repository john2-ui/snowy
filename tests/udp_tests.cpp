/** @file udp_tests.cpp
 *  @brief Datagram boundaries, zero messages, truncation, cancellation, and reuse. */
#include <snowy/snowy.hpp>
#include <array>
#include <cstdlib>
#include <iostream>

/** @brief Fail in all builds. @param ok Required condition. */
void check(bool ok) { if (!ok) std::abort(); }
/** @brief Send three distinct datagrams. @param sender Source. @param peer Destination. */
snowy::task<> send(snowy::udp& sender, snowy::endpoint peer) {
    std::array<std::byte, 32> data{};
    check((co_await sender.send({}, peer)) == 0);
    check((co_await sender.send(data, peer)) == data.size());
    data[0] = std::byte{42};
    check((co_await sender.send(std::span{data}.first(1), peer)) == 1);
}
/** @brief Observe truncation without corrupting the next datagram.
 *  @param receiver Bound socket. @param port Expected sender port. */
snowy::task<> recv(snowy::udp& receiver, std::uint16_t port) {
    std::array<std::byte, 8> data{};
    auto packet = co_await receiver.recv(data);
    check(packet.size == 0 && packet.peer.port() == port);
    bool caught = false;
    try { co_await receiver.recv(data); }
    catch (const std::system_error& e) { caught = e.code() == std::errc::message_size; }
    check(caught);
    packet = co_await receiver.recv(data);
    check(packet.size == 1 && data[0] == std::byte{42});
}
/** @brief Cancel a genuinely pending receive using a structured deadline.
 *  @param loop Owner. @param receiver Bound idle socket. */
snowy::task<> cancel(snowy::loop& loop, snowy::udp& receiver) {
    std::array<std::byte, 8> buffer{};
    bool caught = false;
    try {
        co_await snowy::timeout<snowy::datagram>(loop, std::chrono::milliseconds{1},
            [&](std::stop_token token) { return receiver.recv(buffer, token); });
    } catch (const std::system_error& e) { caught = e.code() == std::errc::timed_out; }
    check(caught);
}
/** @brief Exercise IPv4/IPv6 and canceled socket reuse. */
int main() {
    try {
        snowy::loop loop;
        for (const char* ip : {"127.0.0.1", "::1"}) {
            snowy::udp receiver(loop, {ip, 0}), sender(loop, {ip, 0});
            for (int i = 0; i < 32; ++i) loop.run(cancel(loop, receiver));
            loop.spawn(recv(receiver, sender.local().port()));
            loop.run(send(sender, receiver.local()));
        }
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
