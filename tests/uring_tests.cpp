/** @file uring_tests.cpp
 *  @brief Check ring configuration, registered lifetimes, direct I/O and cancellation. */
#include <snowy/uring.hpp>
#include <snowy/file.hpp>
#include <snowy/when.hpp>
#include <array>
#include <cstdlib>
#include <iostream>

/** @brief Report failing expressions in optimized builds too. @param ok Required invariant.
 *  @param expression Assertion text. @param line Source line. */
void verify(bool ok, const char* expression, int line) {
    if (!ok) {
        std::cerr << "line " << line << ": " << expression << '\n';
        std::abort();
    }
}
#define check(...) verify((__VA_ARGS__), #__VA_ARGS__, __LINE__)
/** @brief Own one exclusively created temporary file. */
struct temp {
    char path[32] = "/tmp/snowy-uring-XXXXXX";
    /** @brief Create a unique file without overwriting user data. */
    temp() {
        int fd = mkstemp(path);
        if (fd < 0) throw std::system_error(errno, std::generic_category());
        ::close(fd);
    }
    /** @brief Remove only the file created by this instance. */
    ~temp() { ::unlink(path); }
};
/** @brief Validate one fixed read. @param files Registered file. @param buffers Registered memory. */
snowy::task<> fixed(snowy::uring::files& files, snowy::uring::buffers& buffers) {
    check((co_await files.read(0, buffers, 0, 0)) == 4096);
}
/** @brief Exercise registered paths and table destruction after cancellation.
 *  @param loop Owner. @param file Existing writable file. */
snowy::task<> run(snowy::loop& loop, snowy::file& file) {
    snowy::uring::memory memory(8192);
    auto data = memory.bytes();
    std::fill(data.begin(), data.end(), std::byte{73});
    const std::array descriptors{file.native_handle()};
    snowy::uring::files files(loop, descriptors);
    std::array<iovec, 2> regions{{{data.data(), 4096}, {data.data() + 4096, 4096}}};
    snowy::uring::buffers buffers(loop, regions);
    check((co_await files.write(0, buffers, 0, 0)) == 4096);
    std::fill(data.begin(), data.end(), std::byte{});
    check((co_await files.read(0, buffers, 1, 0)) == 4096);
    for (auto b : data.subspan(4096)) check(b == std::byte{73});
    check((co_await files.read(0, data.first(4096), 0)) == 4096);
    check((co_await snowy::uring::read(loop, file.native_handle(), data.first(4096), 0)) == 4096);
    std::stop_source stop;
    stop.request_stop();
    bool caught = false;
    try { co_await files.read(0, buffers, 0, 0, stop.get_token()); }
    catch (const std::system_error& e) { caught = e.code() == std::errc::operation_canceled; }
    check(caught);
    caught = false;
    try { auto ignored = files.read(1, data, 0); }
    catch (const std::out_of_range&) { caught = true; }
    check(caught);
    co_await fixed(files, buffers);
}
/** @brief Check idle timers still run on a storage-only ring. @param loop Owner. */
snowy::task<> idle(snowy::loop& loop) { co_await loop.sleep(std::chrono::milliseconds{1}); }
/** @brief Verify soft/hard links, drain and ordered short-read results. @param loop Owner.
 *  @param fd File containing exactly 4096 bytes. */
snowy::task<> chain(snowy::loop& loop, int fd) {
    std::array<std::byte, 2> data{};
    std::array<io_uring_sqe, 3> entries{};
    io_uring_prep_nop(&entries[0]);
    // A short read is an execution-time link failure, even with a positive CQE.
    // Unlike invalid pointers, it cannot fail while the chain is being prepared.
    io_uring_prep_read(&entries[1], fd, data.data(), data.size(), 4095);
    io_uring_prep_nop(&entries[2]);
    auto result = co_await snowy::uring::submit(loop, entries, snowy::uring::link::soft);
    if (result != std::vector<int>({0, 1, -ECANCELED}))
        std::cerr << "soft results: " << result[0] << ' ' << result[1] << ' ' << result[2] << '\n';
    check(result[0] == 0 && result[1] == 1 && result[2] == -ECANCELED);
    result = co_await snowy::uring::submit(loop, entries, snowy::uring::link::hard);
    check(result[0] == 0 && result[1] == 1 && result[2] == 0);
    io_uring_prep_nop(&entries[1]);
    entries[2].flags = IOSQE_IO_DRAIN;
    for (unsigned i = 0; i < 128; ++i) {
        result = co_await snowy::uring::submit(loop, entries, snowy::uring::link::soft);
        check(result == std::vector<int>({0, 0, 0}));
    }
    // The wake source must work again after the last drain has retired.
    co_await loop.sleep(std::chrono::milliseconds{1});
    io_uring_sqe drain{};
    io_uring_prep_nop(&drain);
    drain.flags = IOSQE_IO_DRAIN;
    check((co_await snowy::uring::op(loop, drain)) == 0);
}
/** @brief Run an optional kernel/device mode, reporting unsupported separately.
 *  @param argc Argument count. @param argv Optional direct, sqpoll, or iopoll mode. */
int main(int argc, char** argv) {
    const std::string mode = argc > 1 ? argv[1] : "plain";
    try {
        snowy::uring::options options;
        options.entries = 32;
        options.cq_entries = 64;
        options.budget = 16;
        options.submit_batch = 3;
        if (mode == "sqpoll") options.flags = IORING_SETUP_SQPOLL;
        if (mode == "iopoll") options.flags = IORING_SETUP_IOPOLL;
        snowy::loop loop(options);
        check(snowy::uring::access::ring(loop).sq.ring_entries == 32);
        check(snowy::uring::supports(loop, IORING_OP_READ_FIXED));
        temp temp;
        snowy::file file(loop, temp.path, snowy::file::mode::read_write,
                         mode == "direct" || mode == "iopoll");
        loop.run(run(loop, file));
        loop.run(idle(loop));
        if (mode == "plain" || mode == "sqpoll") loop.run(chain(loop, file.native_handle()));
        bool caught = false;
        options.entries = 0;
        try { snowy::loop invalid(options); } catch (const std::invalid_argument&) { caught = true; }
        check(caught);
    } catch (const std::system_error& e) {
        if (mode != "plain" && (e.code().value() == EOPNOTSUPP || e.code().value() == EINVAL ||
            e.code().value() == EPERM || e.code().value() == ENOSYS)) {
            std::cout << "SKIP " << mode << ": " << e.what() << '\n'; return 77;
        }
        std::cerr << e.what() << '\n'; return 1;
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
