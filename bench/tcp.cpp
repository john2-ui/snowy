/** @file tcp.cpp
 *  @brief Instrumented single-connection loopback echo latency and payload rate. */
#include "common.hpp"
#include <snowy/snowy.hpp>

/** @brief Read complete messages before replying, avoiding crossed partial writes.
 *  @param listener Listening socket. @param size Message bytes. */
snowy::task<> server(snowy::socket& listener, unsigned size) {
    auto peer = co_await listener.accept();
    peer.nodelay();
    std::vector<std::byte> buffer(size);
    for (;;) {
        std::span<std::byte> remaining = buffer;
        while (!remaining.empty()) {
            const auto bytes = co_await peer.read(remaining);
            if (!bytes) {
                if (remaining.size() != size) throw std::runtime_error("partial message at EOF");
                co_return;
            }
            remaining = remaining.subspan(bytes);
        }
        co_await snowy::write_all(peer, buffer);
    }
}

/** @brief Measure full-message RTT with warmup and payload verification.
 *  @param loop Owner. @param address Peer endpoint. @param count Measured roundtrips.
 *  @param size Payload bytes per direction. */
snowy::task<> client(snowy::loop& loop, snowy::endpoint address, unsigned count, unsigned size) {
    auto peer = co_await snowy::socket::connect(loop, address);
    peer.nodelay();
    std::vector<std::byte> send(size, std::byte{42}), receive(size);
    std::vector<double> samples;
    samples.reserve(count);
    const auto warmup = std::min(count, 1000u);
    bench::clock::time_point total;
    for (unsigned i = 0; i < warmup + count; ++i) {
        if (i == warmup) total = bench::clock::now();
        const auto start = bench::clock::now();
        co_await snowy::write_all(peer, send);
        std::span<std::byte> remaining = receive;
        while (!remaining.empty()) {
            const auto bytes = co_await peer.read(remaining);
            if (!bytes) throw std::runtime_error("unexpected EOF");
            remaining = remaining.subspan(bytes);
        }
        const double ns = bench::elapsed(start);
        if (receive != send) throw std::runtime_error("payload mismatch");
        if (i >= warmup) samples.push_back(ns);
    }
    const double ns = bench::elapsed(total);
    peer.shutdown();
    std::sort(samples.begin(), samples.end());
    std::cout << "bytes=" << size << " roundtrips=" << count
              << " RTT ns: p50=" << samples[(samples.size() - 1) / 2]
              << " p99=" << samples[(samples.size() * 99 + 99) / 100 - 1]
              << " max=" << samples.back()
              << " payload MiB/s=" << (2.0 * size * count / (1024 * 1024)) / (ns / 1e9)
              << '\n';
}

/** @brief Run a same-thread echo pair, excluding connection setup from timing. */
int main(int argc, char** argv) {
    try {
        if (argc > 3) throw std::invalid_argument("usage: snowy_tcp [roundtrips] [bytes]");
        const auto count = argc > 1 ? bench::count(argv[1], 1'000'000) : 10'000u;
        const auto size = argc > 2 ? bench::count(argv[2], 65536) : 128u;
        snowy::loop loop;
        auto listener = snowy::socket::listen(loop, {"127.0.0.1", 0});
        loop.spawn(server(listener, size));
        loop.run(client(loop, listener.local(), count, size));
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
