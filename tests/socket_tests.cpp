/** @file socket_tests.cpp
 *  @brief Verify TCP streams, half-close, cancellation, and handle reuse. */
#include <snowy/snowy.hpp>
#include <array>
#include <cstdlib>
#include <iostream>

using namespace std::chrono_literals;

/** @brief Fail in every build mode. @param value Expected condition. */
void check(bool value) { if (!value) std::abort(); }

/** @brief Drain a stream before acknowledging its EOF.
 *  @param listener Listening socket. @param total Received byte count. */
snowy::task<> server(snowy::socket& listener, std::size_t& total) {
    auto peer = co_await listener.accept();
    std::array<std::byte, 997> buffer;
    while (auto count = co_await peer.read(buffer)) {
        for (std::size_t i = 0; i < count; ++i) check(buffer[i] == std::byte{42});
        total += count;
    }
    const std::array reply{std::byte{7}};
    co_await snowy::write_all(peer, reply);
    peer.shutdown();
}

/** @brief Send enough data to require repeated partial I/O.
 *  @param loop Owner. @param address Test listener. */
snowy::task<> client(snowy::loop& loop, snowy::endpoint address) {
    auto peer = co_await snowy::socket::connect(loop, address);
    peer.nodelay();
    std::array<std::byte, 8192> buffer;
    buffer.fill(std::byte{42});
    check((co_await peer.read({})) == 0);
    check((co_await peer.write({})) == 0);
    for (int i = 0; i < 256; ++i) co_await snowy::write_all(peer, buffer);
    peer.shutdown();
    check((co_await peer.read(buffer)) == 1);
    check(buffer[0] == std::byte{7});
    check((co_await peer.read(buffer)) == 0);
}

/** @brief Cancel a pending accept and reuse the same listener repeatedly.
 *  @param listener Test listener. @param token Cancellation token. */
snowy::task<> canceled_accept(snowy::socket& listener, std::stop_token token) {
    bool canceled = false;
    try { (void)co_await listener.accept(token); }
    catch (const std::system_error& e) { canceled = e.code() == std::errc::operation_canceled; }
    check(canceled);
}

/** @brief Cancel after an operation has entered the native queue.
 *  @param loop Owner. @param source Stop source. */
snowy::task<> cancel_later(snowy::loop& loop, std::stop_source& source) {
    co_await loop.sleep(1ms);
    source.request_stop();
}

/** @brief Reject overlapping accept operations before overwriting native state.
 *  @param listener Listener already awaiting a connection. */
snowy::task<> duplicate(snowy::socket& listener) {
    bool rejected = false;
    try { (void)co_await listener.accept(); }
    catch (const std::logic_error&) { rejected = true; }
    check(rejected);
}

/** @brief Keep a connected peer idle while the other endpoint cancels a read.
 *  @param listener Listening socket. @param loop Owner. @param done Peer completion. */
snowy::task<> idle(snowy::socket& listener, snowy::loop& loop, bool& done) {
    auto peer = co_await listener.accept();
    while (!done) co_await loop.sleep(1ms);
}

/** @brief Exercise in-flight receive cancellation.
 *  @param loop Owner. @param address Test listener. @param done Completion indicator. */
snowy::task<> canceled_read(snowy::loop& loop, snowy::endpoint address, bool& done) {
    auto peer = co_await snowy::socket::connect(loop, address);
    std::stop_source source;
    loop.spawn(cancel_later(loop, source));
    std::array<std::byte, 1> buffer;
    bool canceled = false;
    try { (void)co_await peer.read(buffer, source.get_token()); }
    catch (const std::system_error& e) { canceled = e.code() == std::errc::operation_canceled; }
    check(canceled);
    done = true;
}

/** @brief Check both IP families and normal/canceled socket lifetimes. */
int main() {
    try {
        for (const char* host : {"127.0.0.1", "::1"}) {
            snowy::loop loop;
            auto listener = snowy::socket::listen(loop, {host, 0});
            check(listener.local().port() != 0);
            std::size_t total = 0;
            loop.spawn(server(listener, total));
            loop.run(client(loop, listener.local()));
            check(total == 256 * 8192);
            for (int i = 0; i < 32; ++i) {
                std::stop_source source;
                loop.spawn(canceled_accept(listener, source.get_token()));
                loop.spawn(duplicate(listener));
                loop.run(cancel_later(loop, source));
            }
            bool done = false;
            loop.spawn(idle(listener, loop, done));
            loop.run(canceled_read(loop, listener.local(), done));
        }
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
