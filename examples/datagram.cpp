/** @file datagram.cpp
 *  @brief Receive a UDP message with a cooperative deadline. */
#include <snowy/snowy.hpp>
#include <array>
#include <iostream>

/** @brief Send one message. @param sender Source socket. @param peer Destination. */
snowy::task<> send(snowy::udp& sender, snowy::endpoint peer) {
    constexpr std::string_view text = "Hello, UDP!";
    co_await sender.send(std::as_bytes(std::span{text}), peer);
}
/** @brief Receive with timeout, keeping the buffer alive through cleanup.
 *  @param loop Owner. @param receiver Bound socket. */
snowy::task<> recv(snowy::loop& loop, snowy::udp& receiver) {
    std::array<char, 128> buffer{};
    auto packet = co_await snowy::timeout<snowy::datagram>(loop, std::chrono::seconds{1},
        [&](std::stop_token token) { return receiver.recv(std::as_writable_bytes(std::span{buffer}), token); });
    std::cout << std::string_view{buffer.data(), packet.size} << '\n';
}
/** @brief Run a loopback datagram exchange. */
int main() {
    try {
        snowy::loop loop;
        snowy::udp sender(loop, {"127.0.0.1", 0}), receiver(loop, {"127.0.0.1", 0});
        loop.spawn(send(sender, receiver.local()));
        loop.run(recv(loop, receiver));
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
