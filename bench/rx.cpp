/** @file rx.cpp
 *  @brief Continuous receive throughput with an independent blocking sender thread. */
#include "common.hpp"
#include <snowy/snowy.hpp>
#ifdef __linux__
#include <snowy/uring.hpp>
#endif
#ifdef SNOWY_WITH_CONDY
#include <snowy/uring_ops.hpp>
#include <condy/task.hpp>
#include <condy/async_operations.hpp>
#include <condy/buffers.hpp>
#include <condy/provided_buffers.hpp>
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
 *  @param size Buffer bytes. @param elapsed Measured nanoseconds. @param token Deadline.
 *  @param bundle Fill several provided buffers per CQE. */
snowy::task<> receive(snowy::loop& loop, snowy::socket& listener, bool multi,
    unsigned count, unsigned size, double& elapsed, std::stop_token token, bool bundle = false) {
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
        }, token, bundle);
        elapsed = bench::elapsed(start);
#else
        (void)loop;
        (void)bundle;
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
#ifdef SNOWY_WITH_CONDY
/** @brief Receive the same validated stream through Condy. @param runtime Owner.
 *  @param listener Ordinary listening fd. @param multi Multishot mode. @param count Blocks.
 *  @param size Bytes per buffer. @param elapsed Time excluding accept and allocation. */
condy::Coro<> reference_receive(condy::Runtime& runtime, int listener, bool multi,
    unsigned count, unsigned size, double& elapsed) {
    int fd = co_await condy::async_accept(listener, nullptr, nullptr, SOCK_CLOEXEC);
    if (fd < 0) throw std::system_error(-fd, std::generic_category());
    snowy::uring::fd peer(fd);
    std::uint64_t total = 0;
    bool bad = false;
    auto consume = [&](const void* data, std::size_t length) noexcept {
        for (auto byte : std::span{static_cast<const std::byte*>(data), length})
            if (byte != std::byte{42}) bad = true;
        total += length;
    };
    if (multi) {
        condy::ProvidedBufferPool buffers(runtime, 256, size);
        auto consume_result = [&](auto item) noexcept {
            auto& [n, buffer] = item;
            if (n > 0 && static_cast<std::size_t>(n) <= buffer.size()) consume(buffer.data(), static_cast<unsigned>(n));
            else if (n > 0) bad = true;
        };
        const auto start = bench::clock::now();
        for (;;) {
            auto result = co_await condy::async_recv_multishot(fd, buffers, 0, consume_result);
            const int n = result.first;
            consume_result(std::move(result)); // Condy returns the final CQE instead of calling the callback.
            if (!n) break;
            if (n < 0 && n != -ENOBUFS) throw std::system_error(-n, std::generic_category());
        }
        elapsed = bench::elapsed(start);
    } else {
        std::vector<std::byte> data(size);
        const auto start = bench::clock::now();
        for (;;) {
            int n = co_await condy::async_recv(fd, condy::buffer(data), 0);
            if (n < 0) throw std::system_error(-n, std::generic_category());
            if (!n) break;
            consume(data.data(), static_cast<unsigned>(n));
        }
        elapsed = bench::elapsed(start);
    }
    if (bad || total != std::uint64_t{count} * size) throw std::runtime_error("Condy receive mismatch");
}
#endif
/** @brief Run a matched sample. @param mode Receive mode. @param count Blocks. @param size Bytes.
 *  @param reference Select Condy when compiled in. @return Validated payload MiB/s. */
double measure(std::string_view mode, unsigned count, unsigned size, bool reference = false) {
    snowy::loop loop;
#ifdef SNOWY_WITH_CONDY
    std::unique_ptr<condy::Runtime> runtime;
    if (reference) {
        condy::RuntimeOptions options;
        options.sq_size(256).event_interval(64);
        runtime = std::make_unique<condy::Runtime>(options);
    }
#else
    (void)reference;
#endif
    auto listener = snowy::socket::listen(loop, {"127.0.0.1", 0});
    std::exception_ptr error;
    std::jthread sender([&, port = listener.local().port()] { send(port, count, size, error); });
    double elapsed = 0;
#ifdef SNOWY_WITH_CONDY
    if (reference) {
        auto job = condy::co_spawn(*runtime, reference_receive(*runtime, listener.native_handle(), mode == "multi", count, size, elapsed));
        runtime->allow_exit();
        runtime->run();
        job.wait();
    } else
#endif
    loop.run(snowy::timeout<void>(loop, std::chrono::seconds{60}, [&](std::stop_token token) {
        return receive(loop, listener, mode != "single", count, size, elapsed, token, mode == "bundle");
    }));
    sender.join();
    if (error) std::rethrow_exception(error);
    return (double(count) * size / (1024 * 1024)) / (elapsed / 1e9);
}
/** @brief Report a throughput distribution. @param name Implementation/mode. @param samples MiB/s values. */
void report(std::string_view name, std::vector<double> samples) {
    std::sort(samples.begin(), samples.end());
    std::cout << name << " receive MiB/s: median=" << samples[samples.size() / 2]
              << " min=" << samples.front() << " max=" << samples.back() << '\n';
}
/** @brief Run nine samples after warmup. @param argc Argument count.
 *  @param argv single|multi, blocks and bytes. */
int main(int argc, char** argv) {
    try {
        if (argc > 4) throw std::invalid_argument("usage: snowy_rx [single|multi|bundle] [blocks] [bytes]");
        const std::string_view mode = argc > 1 ? argv[1] : "single";
        if (mode != "single" && mode != "multi" && mode != "bundle") throw std::invalid_argument("unknown receive mode");
#ifdef SNOWY_WITH_CONDY
        if (mode == "bundle") throw std::invalid_argument("Condy RX comparison supports single or multi");
        std::vector<double> references;
#endif
        const auto count = argc > 2 ? bench::count(argv[2]) : 10000u;
        const auto size = argc > 3 ? bench::count(argv[3], 65536) : 2048u;
        std::vector<double> samples;
        for (unsigned i = 0; i < 10; ++i) {
            double a;
#ifdef SNOWY_WITH_CONDY
            double b;
            if (i % 2) { a = measure(mode, count, size); b = measure(mode, count, size, true); }
            else { b = measure(mode, count, size, true); a = measure(mode, count, size); }
            if (i) references.push_back(b);
#else
            a = measure(mode, count, size);
#endif
            if (i) samples.push_back(a);
        }
        report("snowy", samples);
#ifdef SNOWY_WITH_CONDY
        report("condy", references);
#endif
        std::cout << "mode=" << mode << " bytes=" << size
                  << " (payload validation included; loopback, not NIC throughput)\n";
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
