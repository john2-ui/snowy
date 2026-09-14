/** @file multi_tests.cpp
 *  @brief Multishot readiness, timeout and pipe-read completion/cancellation checks. */
#include <snowy/uring_multi.hpp>
#include <snowy/uring_ops.hpp>
#include <snowy/when.hpp>
#include <poll.h>
#include <array>
#include <cstdlib>
#include <iostream>
using namespace std::chrono_literals;

/** @brief Assert in release builds too. @param ok Required condition. */
void check(bool ok) { if (!ok) std::abort(); }
/** @brief Own an isolated nonblocking pipe. */
struct pipe_pair {
    /** @brief Create descriptors before binding their RAII owners. */
    static std::array<int, 2> open() {
        std::array<int, 2> fds;
        if (pipe2(fds.data(), O_CLOEXEC | O_NONBLOCK)) throw std::system_error(errno, std::generic_category());
        return fds;
    }
    const std::array<int, 2> fds = open();
    snowy::uring::fd reader{fds[0]}, writer{fds[1]};
};
/** @brief Produce readiness in separate loop iterations. @param loop Owner. @param fd Writer. */
snowy::task<> produce(snowy::loop& loop, int fd) {
    for (int i = 0; i < 3; ++i) {
        co_await loop.sleep(1ms);
        check(::write(fd, "x", 1) == 1);
    }
}
/** @brief Cancel an already suspended native request. @param loop Owner. @param stop Source. */
snowy::task<> cancel(snowy::loop& loop, std::stop_source& stop) {
    co_await loop.sleep(1ms);
    stop.request_stop();
}
/** @brief Repeated readiness, callback failure, and in-flight cancellation. @param loop Owner. */
snowy::task<> polling(snowy::loop& loop) {
    pipe_pair pipe;
    unsigned seen = 0;
    co_await snowy::when_all(loop,
        snowy::uring::watch(loop, pipe.reader.get(), POLLIN, [&](unsigned flags) {
            check(flags & POLLIN);
            char byte;
            check(::read(pipe.reader.get(), &byte, 1) == 1 && byte == 'x');
            return ++seen < 3;
        }), produce(loop, pipe.writer.get()));
    check(seen == 3);
    check(::write(pipe.writer.get(), "x", 1) == 1);
    bool caught = false;
    try { co_await snowy::uring::watch(loop, pipe.reader.get(), POLLIN, [](unsigned) -> bool { throw std::runtime_error("callback"); }); }
    catch (const std::runtime_error& e) { caught = std::string_view(e.what()) == "callback"; }
    check(caught);
    char byte;
    check(::read(pipe.reader.get(), &byte, 1) == 1);
    std::stop_source stop;
    loop.spawn(cancel(loop, stop));
    caught = false;
    try { co_await snowy::uring::watch(loop, pipe.reader.get(), POLLIN, [](unsigned) { return true; }, stop.get_token()); }
    catch (const std::system_error& e) { caught = e.code() == std::errc::operation_canceled; }
    check(caught);
}
/** @brief Finite and callback-stopped timers plus failure/cancellation cleanup. @param loop Owner. */
snowy::task<> timers(snowy::loop& loop) {
    unsigned seen = 0;
    co_await snowy::uring::ticks(loop, 1ms, [&] { ++seen; return true; }, 3);
    check(seen == 3);
    seen = 0;
    co_await snowy::uring::ticks(loop, 1ms, [&] { return ++seen != 3; });
    check(seen == 3);
    bool caught = false;
    try { co_await snowy::uring::ticks(loop, 1ms, []() -> bool { throw std::runtime_error("callback"); }); }
    catch (const std::runtime_error& e) { caught = std::string_view(e.what()) == "callback"; }
    check(caught);
    std::stop_source stop;
    loop.spawn(cancel(loop, stop));
    caught = false;
    try { co_await snowy::uring::ticks(loop, 1s, [] { return true; }, 0, stop.get_token()); }
    catch (const std::system_error& e) { caught = e.code() == std::errc::operation_canceled; }
    check(caught);
    caught = false;
    try { co_await snowy::uring::ticks(loop, 0ns, [] { return true; }); }
    catch (const std::invalid_argument&) { caught = true; }
    check(caught);
}
/** @brief Read several buffers and EOF, then drain callback failure/cancellation. @param loop Owner. */
snowy::task<> reading(snowy::loop& loop) {
    pipe_pair pipe;
    snowy::uring::provided buffers(loop, 2, 64);
    std::array<char, 512> data{};
    data.fill('x');
    check(::write(pipe.writer.get(), data.data(), data.size()) == 512);
    check(::close(pipe.writer.release()) == 0);
    unsigned total = 0;
    co_await snowy::uring::read(loop, pipe.reader.get(), buffers, [&](snowy::uring::provided::chunk value) {
        for (auto byte : value.bytes()) check(byte == std::byte{'x'});
        total += static_cast<unsigned>(value.bytes().size());
        return true;
    });
    check(total == 512);
    pipe_pair other;
    check(::write(other.writer.get(), "x", 1) == 1);
    bool caught = false;
    try { co_await snowy::uring::read(loop, other.reader.get(), buffers, [](auto) -> bool { throw std::runtime_error("callback"); }); }
    catch (const std::runtime_error& e) { caught = std::string_view(e.what()) == "callback"; }
    check(caught);
    std::stop_source stop;
    loop.spawn(cancel(loop, stop));
    caught = false;
    try { co_await snowy::uring::read(loop, other.reader.get(), buffers, [](auto) { return true; }, stop.get_token()); }
    catch (const std::system_error& e) { caught = e.code() == std::errc::operation_canceled; }
    check(caught);
}
/** @brief Select capability so old-kernel skips do not hide working paths.
 *  @param argc Count. @param argv watch|ticks|readmulti. */
int main(int argc, char** argv) {
    try {
        snowy::loop loop;
        const std::string_view mode = argc > 1 ? argv[1] : "watch";
        if (mode == "watch") loop.run(polling(loop));
        else if (mode == "ticks") loop.run(timers(loop));
        else if (mode == "readmulti") loop.run(reading(loop));
        else throw std::invalid_argument("unknown test mode");
    } catch (const std::system_error& e) {
        std::cerr << e.what() << '\n';
        if (!std::getenv("SNOWY_REQUIRE_ADVANCED") &&
            (e.code().value() == EINVAL || e.code().value() == EOPNOTSUPP || e.code().value() == ENOSYS)) return 77;
        return 1;
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
