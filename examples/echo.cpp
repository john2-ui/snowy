/** @file echo.cpp
 *  @brief Self-contained TCP echo: one server and one client on the same loop. */
#include <snowy/snowy.hpp>
#include <array>
#include <iostream>
#include <string_view>

/** @brief Echo until the client half-closes. @param listener Bound listening socket. */
snowy::task<> serve(snowy::socket& listener) {
    auto peer = co_await listener.accept();
    std::array<std::byte, 1024> buffer;
    while (auto count = co_await peer.read(buffer))
        co_await snowy::write_all(peer, std::span{buffer}.first(count));
}

/** @brief Send a message and verify the echo.
 *  @param loop Owner. @param address Server endpoint. */
snowy::task<> client(snowy::loop& loop, snowy::endpoint address) {
    auto peer = co_await snowy::socket::connect(loop, address);
    constexpr std::string_view message = "Hello, TCP!";
    co_await snowy::write_all(peer, std::as_bytes(std::span{message}));
    peer.shutdown();
    std::array<char, message.size()> response{};
    std::span<std::byte> remaining = std::as_writable_bytes(std::span{response});
    while (!remaining.empty()) {
        const auto count = co_await peer.read(remaining);
        if (!count) throw std::runtime_error("unexpected EOF");
        remaining = remaining.subspan(count);
    }
    if (std::string_view{response.data(), response.size()} != message)
        throw std::runtime_error("echo mismatch");
    std::cout << message << '\n';
}

/** @brief Bind an ephemeral loopback port, start both peers, and drain their work. */
int main() {
    try {
        snowy::loop loop;
        auto listener = snowy::socket::listen(loop, {"127.0.0.1", 0});
        loop.spawn(serve(listener));
        loop.run(client(loop, listener.local()));
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
