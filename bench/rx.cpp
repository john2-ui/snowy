/** @file rx.cpp
 *  @brief Continuous receive throughput with an independent blocking sender thread. */
#include "common.hpp"
#include <snowy/snowy.hpp>
#ifdef __linux__
#include <snowy/uring.hpp>
#endif

/** @brief Feed exactly count blocks, handling short sends. @param port Loopback port.
 *  @param count Blocks. @param size Block bytes. @param error Sender failure output. */
void send(std::uint16_t port, unsigned count, unsigned size, std::exception_ptr& error) {
    auto fd = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    try {
        if (fd == snowy::detail::invalid_socket) throw std::runtime_error("sender socket failed");
#ifdef __APPLE__
        const int enabled = 1;
        if (setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &enabled, sizeof(enabled)))
            throw std::runtime_error("SO_NOSIGPIPE failed");
#endif
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = htons(port);
        if (::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)))
            throw std::runtime_error("sender connect failed");
        std::vector<char> data(size, 42);
        for (unsigned i = 0; i < count; ++i) {
            unsigned offset = 0;
            while (offset != size) {
#ifdef __linux__
                constexpr int flags = MSG_NOSIGNAL;
#else
                constexpr int flags = 0;
#endif
                const auto n = ::send(fd, data.data() + offset, static_cast<int>(size - offset), flags);
                if (n <= 0) throw std::runtime_error("sender send failed");
                offset += static_cast<unsigned>(n);
            }
        }
    } catch (...) { error = std::current_exception(); }
    snowy::detail::close(fd);
}
/** @brief Count and validate payload on the receiving loop. @param loop Owner.
 *  @param listener Acceptor. @param multi Multishot mode. @param count Blocks sent.
 *  @param size Buffer bytes. @param elapsed Measured nanoseconds. @param token Deadline. */
snowy::task<> receive(snowy::loop& loop, snowy::socket& listener, bool multi,
    unsigned count, unsigned size, double& elapsed, std::stop_token token) {
    auto peer = co_await listener.accept(token);
    std::uint64_t total = 0;
    auto consume = [&](std::span<const std::byte> data) {
        for (auto b : data) if (b != std::byte{42}) throw std::runtime_error("payload mismatch");
        total += data.size();
    };
    if (multi) {
#ifdef __linux__
        snowy::uring::provided buffers(loop, 256, size);
        const auto start = bench::clock::now();
        co_await snowy::uring::recv(peer, buffers, [&](snowy::uring::provided::chunk chunk) {
            consume(chunk.bytes()); return true;
        }, token);
        elapsed = bench::elapsed(start);
#else
        (void)loop;
        throw std::invalid_argument("multishot requires Linux");
#endif
    } else {
        std::vector<std::byte> data(size);
        const auto start = bench::clock::now();
        while (auto n = co_await peer.read(data, token)) consume(std::span(data).first(n));
        elapsed = bench::elapsed(start);
    }
    if (total != std::uint64_t{count} * size) throw std::runtime_error("received byte count mismatch");
}
/** @brief Run nine samples after warmup. @param argc Argument count.
 *  @param argv single|multi, blocks and bytes. */
int main(int argc, char** argv) {
    try {
        if (argc > 4) throw std::invalid_argument("usage: snowy_rx [single|multi] [blocks] [bytes]");
        const std::string_view mode = argc > 1 ? argv[1] : "single";
        if (mode != "single" && mode != "multi") throw std::invalid_argument("unknown receive mode");
        const auto count = argc > 2 ? bench::count(argv[2]) : 10000u;
        const auto size = argc > 3 ? bench::count(argv[3], 65536) : 2048u;
        std::vector<double> samples;
        for (unsigned i = 0; i < 10; ++i) {
            snowy::loop loop;
            auto listener = snowy::socket::listen(loop, {"127.0.0.1", 0});
            std::exception_ptr error;
            std::jthread sender([&, port = listener.local().port()] { send(port, count, size, error); });
            double elapsed = 0;
            loop.run(snowy::timeout<void>(loop, std::chrono::seconds{60}, [&](std::stop_token token) {
                return receive(loop, listener, mode == "multi", count, size, elapsed, token);
            }));
            sender.join();
            if (error) std::rethrow_exception(error);
            if (i) samples.push_back((double(count) * size / (1024 * 1024)) / (elapsed / 1e9));
        }
        std::sort(samples.begin(), samples.end());
        std::cout << mode << " receive MiB/s: median=" << samples[4] << " min=" << samples.front()
                  << " max=" << samples.back() << " bytes=" << size
                  << " (payload validation included; loopback, not NIC throughput)\n";
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
