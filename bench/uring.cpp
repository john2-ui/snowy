/** @file uring.cpp
 *  @brief Compare single-shot NOP completion cost against a raw liburing loop. */
#include "common.hpp"
#include <snowy/detail/io.hpp>
#include <liburing.h>
#include <cerrno>

/** @brief Own a ring configured like Snowy's Linux backend. */
struct ring {
    io_uring queue{};
    /** @brief Allocate 256 slots, falling back for pre-SINGLE_ISSUER kernels. */
    ring() {
        io_uring_params params{};
        params.flags = IORING_SETUP_SINGLE_ISSUER;
        int error = io_uring_queue_init_params(256, &queue, &params);
        if (error == -EINVAL) error = io_uring_queue_init(256, &queue, 0);
        if (error < 0) throw std::system_error(-error, std::generic_category());
    }
    /** @brief Release the fully drained ring. */
    ~ring() { io_uring_queue_exit(&queue); }
    ring(const ring&) = delete;
    ring& operator=(const ring&) = delete;
    /** @brief Queue one NOP. @param lane Stable stream identifier. */
    void nop(unsigned lane) {
        auto* entry = io_uring_get_sqe(&queue);
        if (!entry) throw std::runtime_error("benchmark SQ full");
        io_uring_prep_nop(entry);
        io_uring_sqe_set_data64(entry, lane);
    }
};

/** @brief Time a raw completion loop, excluding ring allocation.
 *  @param count Operations per lane. @param depth Concurrent lanes, at most 128. */
double raw(unsigned count, unsigned depth) {
    ring ring;
    std::vector<unsigned> done(depth);
    const auto start = bench::clock::now();
    for (unsigned lane = 0; lane < depth; ++lane) ring.nop(lane);
    std::uint64_t left = std::uint64_t{count} * depth;
    while (left) {
        const int error = io_uring_submit_and_wait(&ring.queue, 1);
        if (error < 0 && error != -EINTR)
            throw std::system_error(-error, std::generic_category());
        io_uring_cqe* batch[128];
        const unsigned size = io_uring_peek_batch_cqe(&ring.queue, batch, 128);
        for (unsigned i = 0; i < size; ++i) {
            if (batch[i]->res != 0) throw std::runtime_error("NOP failed");
            const auto lane = static_cast<unsigned>(io_uring_cqe_get_data64(batch[i]));
            --left;
            if (++done[lane] != count) ring.nop(lane);
        }
        io_uring_cq_advance(&ring.queue, size);
    }
    return bench::elapsed(start) / (static_cast<double>(count) * depth);
}

/** @brief Keep one NOP outstanding per lane. @param loop Owner. @param count NOPs. */
snowy::task<> lane(snowy::loop& loop, unsigned count) {
    for (unsigned i = 0; i < count; ++i)
        co_await snowy::detail::io{loop, snowy::detail::opcode::nop};
}

/** @brief Time the wrapped path, excluding ring and root allocation.
 *  @param count NOPs per lane. @param depth Concurrent lanes. */
double wrapped(unsigned count, unsigned depth) {
    snowy::loop loop;
    for (unsigned i = 0; i < depth; ++i) loop.spawn(lane(loop, count));
    const auto start = bench::clock::now();
    loop.run();
    return bench::elapsed(start) / (static_cast<double>(count) * depth);
}

/** @brief Alternate order across nine samples after a discarded warmup pair. */
int main(int argc, char** argv) {
    try {
        if (argc > 3) throw std::invalid_argument("usage: snowy_uring [count-per-lane] [depth:1..128]");
        const auto count = argc > 1 ? bench::count(argv[1]) : 100'000u;
        const auto depth = argc > 2 ? bench::count(argv[2], 128) : 32u;
        std::vector<double> baseline, snowy;
        for (unsigned i = 0; i < 10; ++i) {
            double a, b;
            if (i % 2) { b = wrapped(count, depth); a = raw(count, depth); }
            else { a = raw(count, depth); b = wrapped(count, depth); }
            if (i) { baseline.push_back(a); snowy.push_back(b); }
        }
        std::cout << "NOPs/lane=" << count << " depth=" << depth << '\n';
        bench::report("liburing", baseline);
        bench::report("snowy", snowy);
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
