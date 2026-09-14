/** @file uring_net_tests.cpp
 *  @brief Native multishot exhaustion, callback failure, cancellation and zero-copy tests. */
#include <snowy/snowy.hpp>
#include <snowy/uring.hpp>
#include <array>
#include <cstdlib>
#include <iostream>

/** @brief Fail in all build modes. @param ok Required invariant. */
void check(bool ok) { if (!ok) std::abort(); }
/** @brief Send a deterministic stream. @param loop Owner. @param address Listener.
 *  @param zc Use registered zero-copy sends when true. */
snowy::task<> send(snowy::loop& loop, snowy::endpoint address, bool zc) {
    auto peer = co_await snowy::socket::connect(loop, address);
    snowy::uring::memory storage(4096);
    auto data = storage.bytes();
    std::fill(data.begin(), data.end(), std::byte{42});
    std::array<iovec, 1> regions{{{data.data(), data.size()}}};
    snowy::uring::buffers registered(loop, regions);
    for (unsigned i = 0; i < 128; ++i) {
        if (zc) {
            const auto n = co_await snowy::uring::send_zc(peer, registered, 0);
            check(n > 0 && n <= data.size());
            auto rest = data.subspan(n);
            while (!rest.empty()) {
                const auto sent = co_await snowy::uring::send_zc(peer, rest);
                check(sent > 0);
                rest = rest.subspan(sent);
            }
        } else co_await snowy::write_all(peer, data);
        // Reuse is safe only after the release notification, not the first CQE.
        std::fill(data.begin(), data.end(), std::byte{42});
    }
    peer.shutdown();
}
/** @brief Drain a deterministic stream. @param loop Owner. @param listener Acceptor.
 *  @param multi Use two leased buffers, repeatedly exhausting the ring. */
snowy::task<> receive(snowy::loop& loop, snowy::socket& listener, bool multi) {
    auto peer = co_await listener.accept();
    std::size_t bytes = 0;
    if (multi) {
        snowy::uring::provided buffers(loop, 2, 4096);
        std::vector<snowy::uring::provided::chunk> held;
        held.reserve(2);
        co_await snowy::uring::recv(peer, buffers, [&](snowy::uring::provided::chunk chunk) {
            for (auto b : chunk.bytes()) check(b == std::byte{42});
            bytes += chunk.bytes().size();
            held.push_back(std::move(chunk));
            if (held.size() == 2) loop.post([&held] { held.clear(); });
            return true;
        });
        // Drain the posted release before its captured state leaves scope.
        co_await loop.schedule();
        held.clear();
    } else {
        std::array<std::byte, 4096> data;
        while (auto n = co_await peer.read(data)) {
            for (auto b : std::span(data).first(n)) check(b == std::byte{42});
            bytes += n;
        }
    }
    check(bytes == 128 * 4096);
}
/** @brief Make sequential short-lived clients. @param loop Owner. @param address Listener. */
snowy::task<> connect(snowy::loop& loop, snowy::endpoint address) {
    for (unsigned i = 0; i < 128; ++i) { auto peer = co_await snowy::socket::connect(loop, address); }
}
/** @brief Stop a multishot accept normally, then cancel and reuse the listener.
 *  @param loop Owner. @param listener Acceptor. */
snowy::task<> accept(snowy::loop& loop, snowy::socket& listener) {
    unsigned count = 0;
    co_await snowy::uring::accept(listener, [&](snowy::socket) { return ++count != 128; });
    check(count == 128);
    bool caught = false;
    try {
        co_await snowy::timeout<void>(loop, std::chrono::milliseconds{1}, [&](std::stop_token token) {
            return snowy::uring::accept(listener, [](snowy::socket) { return true; }, token);
        });
    } catch (const std::system_error& e) { caught = e.code() == std::errc::timed_out; }
    check(caught);
}
/** @brief Verify thrown callbacks drain the multishot request.
 *  @param loop Owner. @param listener Acceptor. */
snowy::task<> fail(snowy::loop& loop, snowy::socket& listener) {
    auto peer = co_await listener.accept();
    snowy::uring::provided buffers(loop, 8, 4096);
    bool caught = false;
    try {
        co_await snowy::uring::recv(peer, buffers, [](snowy::uring::provided::chunk) -> bool {
            throw std::runtime_error("consumer");
        });
    } catch (const std::runtime_error& e) { caught = std::string_view(e.what()) == "consumer"; }
    check(caught);
    std::array<std::byte, 4096> data;
    while (co_await peer.read(data)) {}
}
/** @brief Race cancellation with send/release CQEs, then overwrite borrowed memory.
 *  @param loop Owner. @param address Listener. */
snowy::task<> cancel_zc(snowy::loop& loop, snowy::endpoint address) {
    auto peer = co_await snowy::socket::connect(loop, address);
    const int size = 4096;
    check(setsockopt(peer.native_handle(), SOL_SOCKET, SO_SNDBUF, &size, sizeof(size)) == 0);
    snowy::uring::memory memory(65536);
    auto data = memory.bytes();
    const iovec region{data.data(), data.size()};
    snowy::uring::buffers buffers(loop, std::span(&region, 1));
    for (unsigned i = 0; i < 16; ++i) {
        std::fill(data.begin(), data.end(), std::byte{42});
        std::stop_source stop;
        snowy::event posted(loop);
        loop.post([&] { stop.request_stop(); posted.set(); });
        try { check((co_await snowy::uring::send_zc(peer, buffers, 0, stop.get_token())) <= data.size()); }
        catch (const std::system_error& e) { check(e.code() == std::errc::operation_canceled); }
        co_await posted.join();
        std::fill(data.begin(), data.end(), std::byte{7});
        co_await loop.sleep(std::chrono::milliseconds{1});
    }
    peer.shutdown();
}
/** @brief Validate all bytes accepted before cancellation, without assuming rollback.
 *  @param listener Acceptor. */
snowy::task<> drain(snowy::socket& listener) {
    auto peer = co_await listener.accept();
    std::array<std::byte, 8192> data;
    while (auto n = co_await peer.read(data))
        for (auto b : std::span(data).first(n)) check(b == std::byte{42});
}
/** @brief Run exactly one feature so unsupported tests are visible separately.
 *  @param argc Argument count. @param argv recv, accept, zc, or fail. */
int main(int argc, char** argv) {
    const std::string mode = argc > 1 ? argv[1] : "recv";
    try {
        snowy::loop loop;
        if (mode == "zc" && !snowy::uring::supports(loop, IORING_OP_SEND_ZC)) {
            if (std::getenv("SNOWY_REQUIRE_ADVANCED")) throw std::runtime_error("SEND_ZC unavailable");
            std::cout << "SKIP SEND_ZC opcode\n"; return 77;
        }
        auto listener = snowy::socket::listen(loop, {"127.0.0.1", 0});
        if (mode == "accept") {
            loop.spawn(connect(loop, listener.local()));
            loop.run(accept(loop, listener));
        } else {
            loop.spawn(send(loop, listener.local(), mode == "zc"));
            if (mode == "fail") loop.run(fail(loop, listener));
            else loop.run(receive(loop, listener, mode == "recv"));
        }
        if (mode == "zc") {
            loop.spawn(cancel_zc(loop, listener.local()));
            loop.run(drain(listener));
        }
    } catch (const std::system_error& e) {
        if (!std::getenv("SNOWY_REQUIRE_ADVANCED") &&
            (e.code().value() == EINVAL || e.code().value() == EOPNOTSUPP || e.code().value() == ENOSYS)) {
            std::cout << "SKIP " << mode << ": " << e.what() << '\n'; return 77;
        }
        std::cerr << e.what() << '\n'; return 1;
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
