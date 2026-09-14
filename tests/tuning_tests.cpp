/** @file tuning_tests.cpp
 *  @brief Ring descriptor registration, io-wq tuning and registration cloning. */
#include <snowy/uring.hpp>
#include <array>
#include <cstdlib>
#include <future>
#include <iostream>

/** @brief Assert even under optimization. @param ok Required condition. */
void check(bool ok) { if (!ok) std::abort(); }
/** @brief Exercise submissions and the wake source. @param loop Owner. */
snowy::task<> run(snowy::loop& loop) {
    for (int i = 0; i < 100; ++i) co_await snowy::uring::nop(loop);
    co_await loop.sleep(std::chrono::milliseconds{1});
}
/** @brief Clone stable memory from a foreign loop, then read through its registration.
 *  @param loop Destination. @param source Externally synchronized source table. */
snowy::task<> copied(snowy::loop& loop, const snowy::uring::buffers& source) {
    snowy::uring::buffers copy(loop, source);
    int fd = ::open("/dev/zero", O_RDONLY | O_CLOEXEC);
    if (fd < 0) throw std::system_error(errno, std::generic_category());
    struct close_fd {
        int fd;
        /** @brief Close the test descriptor after all awaits finish. */
        ~close_fd() { ::close(fd); }
    } close{fd};
    io_uring_sqe sqe{};
    auto data = copy.at(0);
    io_uring_prep_read_fixed(&sqe, fd, data.data(), static_cast<unsigned>(data.size()), 0, 0);
    check((co_await snowy::uring::op(loop, sqe, {}, nullptr, &copy)) == data.size());
    for (auto byte : data) check(byte == std::byte{});
}
/** @brief Run one optional tuning path. @param argc Count. @param argv wq, ringfd, clone or napi. */
int main(int argc, char** argv) {
    const std::string mode = argc > 1 ? argv[1] : "wq";
    try {
        snowy::uring::options options;
        options.entries = 32;
        options.submit_batch = 3;
        options.register_fd = mode == "ringfd";
        options.single_issuer = mode != "clone";
        snowy::loop loop(options);
        if (mode == "wq") {
            auto old = snowy::uring::workers(loop, {2, 3});
            check(snowy::uring::workers(loop, {0, 0}) == std::array<unsigned, 2>{2, 3});
            cpu_set_t cpus;
            check(sched_getaffinity(0, sizeof(cpus), &cpus) == 0);
            snowy::uring::affinity(loop, &cpus);
            snowy::uring::affinity(loop, nullptr);
            options.wq_fd = snowy::uring::access::fd(loop);
            snowy::loop shared(options);
            shared.run(run(shared));
            snowy::uring::workers(loop, old);
        } else if (mode == "napi") {
            snowy::uring::napi(loop, 10);
            snowy::uring::napi(loop, 0);
        } else if (mode == "clone") {
            snowy::uring::memory data(4096);
            std::fill(data.bytes().begin(), data.bytes().end(), std::byte{42});
            std::array<iovec, 1> regions{{{data.bytes().data(), data.bytes().size()}}};
            snowy::uring::buffers source(loop, regions);
            auto result = std::async(std::launch::async, [&] {
                snowy::loop target;
                target.run(copied(target, source));
            });
            result.get();
        }
        loop.run(run(loop));
    } catch (const std::system_error& e) {
        std::cerr << e.what() << '\n';
        if (mode != "wq" && !std::getenv("SNOWY_REQUIRE_ADVANCED")
            && (e.code().value() == EINVAL || e.code().value() == EOPNOTSUPP || e.code().value() == ENOSYS)) return 77;
        return 1;
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
