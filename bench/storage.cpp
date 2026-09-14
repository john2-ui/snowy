/** @file storage.cpp
 *  @brief Alternate raw liburing and Snowy under identical storage modes and offsets. */
#include "common.hpp"
#include <snowy/file.hpp>
#include <snowy/uring.hpp>

/** @brief Shared workload; cache policy and native setup are identical in each pair. */
struct workload {
    const char* path;
    unsigned count, depth, size;
    bool direct, fixed;
    snowy::uring::options options;
};
/** @brief Advance a deterministic per-lane random stream. @param seed Mutable seed.
 *  @param blocks Available file blocks. @param size Block bytes. */
std::uint64_t offset(std::uint64_t& seed, std::uint64_t blocks, unsigned size) {
    seed ^= seed << 13; seed ^= seed >> 7; seed ^= seed << 17;
    return (seed % blocks) * size;
}
/** @brief Keep one wrapped read outstanding. @param loop Owner. @param fd File.
 *  @param files Optional fixed file table. @param buffers Optional fixed buffer table.
 *  @param data Lane buffer. @param lane Index. @param work Workload. @param blocks File blocks.
 *  @param checksum Accumulated first-byte checksum. */
snowy::task<> lane(snowy::loop& loop, int fd, snowy::uring::files* files, snowy::uring::buffers* buffers,
    std::span<std::byte> data, unsigned lane, const workload& work, std::uint64_t blocks, std::uint64_t& checksum) {
    std::uint64_t seed = lane + 1;
    for (unsigned i = 0; i < work.count; ++i) {
        const auto pos = offset(seed, blocks, work.size);
        const auto n = files ? co_await files->read(0, *buffers, lane, pos)
                             : co_await snowy::uring::read(loop, fd, data, pos);
        if (n != data.size()) throw std::runtime_error("short file read");
        checksum += std::to_integer<unsigned>(data.front());
    }
}
/** @brief Measure one implementation, excluding allocation/open/registration.
 *  @param work Shared workload. @param raw Use direct liburing completion processing.
 *  @param checksum Output checksum, reset for this run. */
double measure(const workload& work, bool raw, std::uint64_t& checksum) {
    checksum = 0;
    snowy::loop loop(work.options);
    snowy::file file(loop, work.path, snowy::file::mode::read, work.direct);
    const auto blocks = std::filesystem::file_size(work.path) / work.size;
    if (!blocks) throw std::invalid_argument("file smaller than a block");
    snowy::uring::memory memory(std::size_t{work.size} * work.depth);
    std::vector<iovec> regions;
    for (unsigned i = 0; i < work.depth; ++i)
        regions.push_back({memory.bytes().data() + std::size_t{i} * work.size, work.size});
    const int fd = file.native_handle();
    std::unique_ptr<snowy::uring::files> files;
    std::unique_ptr<snowy::uring::buffers> buffers;
    if (work.fixed) {
        files = std::make_unique<snowy::uring::files>(loop, std::span(&fd, 1));
        buffers = std::make_unique<snowy::uring::buffers>(loop, regions);
    }
    if (!raw) {
        for (unsigned i = 0; i < work.depth; ++i)
            loop.spawn(lane(loop, fd, files.get(), buffers.get(),
                {static_cast<std::byte*>(regions[i].iov_base), work.size}, i, work, blocks, checksum));
        const auto start = bench::clock::now();
        loop.run();
        return bench::elapsed(start) / (double(work.count) * work.depth);
    }
    auto& ring = snowy::uring::access::ring(loop);
    std::vector<unsigned> done(work.depth);
    std::vector<std::uint64_t> seeds(work.depth);
    for (unsigned i = 0; i < work.depth; ++i) seeds[i] = i + 1;
    auto submit = [&](unsigned i) {
        auto* sqe = io_uring_get_sqe(&ring);
        if (!sqe) throw std::runtime_error("raw SQ full");
        const auto pos = offset(seeds[i], blocks, work.size);
        if (work.fixed) {
            io_uring_prep_read_fixed(sqe, 0, regions[i].iov_base, work.size, pos, static_cast<int>(i));
            sqe->flags |= IOSQE_FIXED_FILE;
        } else io_uring_prep_read(sqe, fd, regions[i].iov_base, work.size, pos);
        io_uring_sqe_set_data64(sqe, i + 1);
    };
    const auto start = bench::clock::now();
    for (unsigned i = 0; i < work.depth; ++i) submit(i);
    unsigned left = work.depth;
    std::error_code error;
    while (left) {
        const int r = io_uring_submit_and_wait(&ring, 1);
        if (r < 0 && r != -EINTR) throw std::system_error(-r, std::generic_category());
        io_uring_cqe* batch[128];
        const auto n = io_uring_peek_batch_cqe(&ring, batch, 128);
        for (unsigned j = 0; j < n; ++j) {
            const auto id = io_uring_cqe_get_data64(batch[j]);
            if (!id) throw std::runtime_error("unexpected raw wake CQE");
            const auto i = static_cast<unsigned>(id - 1);
            --left;
            if (batch[j]->res != static_cast<int>(work.size))
                error = {batch[j]->res < 0 ? -batch[j]->res : EIO, std::generic_category()};
            else checksum += std::to_integer<unsigned>(*static_cast<std::byte*>(regions[i].iov_base));
            // Retire all published reads before unregistering or freeing memory.
            if (++done[i] != work.count && !error) { submit(i); ++left; }
        }
        io_uring_cq_advance(&ring, n);
    }
    if (error) throw std::system_error(error, "raw read");
    return bench::elapsed(start) / (double(work.count) * work.depth);
}
/** @brief Read only an existing file; never create, truncate or evict its cache.
 *  @param argc Argument count. @param argv Mode, path, count, depth and bytes. */
int main(int argc, char** argv) {
    try {
        if (argc < 3 || argc > 6) throw std::invalid_argument(
            "usage: snowy_storage buffered|direct|fixed|fixed-direct|iopoll|sqpoll path [count/lane] [depth] [bytes]");
        const std::string_view mode = argv[1];
        workload work{argv[2], argc > 3 ? bench::count(argv[3]) : 10000u,
            argc > 4 ? bench::count(argv[4], 128) : 32u,
            argc > 5 ? bench::count(argv[5], 65536) : 4096u, false, false, {}};
        if (mode != "buffered" && mode != "direct" && mode != "fixed" && mode != "fixed-direct" &&
            mode != "iopoll" && mode != "sqpoll") throw std::invalid_argument("unknown storage mode");
        work.direct = mode == "direct" || mode == "fixed-direct" || mode == "iopoll" || mode == "sqpoll";
        work.fixed = mode == "fixed" || mode == "fixed-direct" || mode == "iopoll" || mode == "sqpoll";
        if (work.direct && work.size % 4096) throw std::invalid_argument("direct mode requires 4096-byte multiples");
        if (mode == "iopoll") work.options.flags = IORING_SETUP_IOPOLL;
        if (mode == "sqpoll") work.options.flags = IORING_SETUP_SQPOLL;
        std::vector<double> raw, wrapped;
        std::uint64_t a = 0, b = 0;
        for (unsigned i = 0; i < 10; ++i) {
            double x, y;
            if (i % 2) { y = measure(work, false, b); x = measure(work, true, a); }
            else { x = measure(work, true, a); y = measure(work, false, b); }
            if (a != b) throw std::runtime_error("checksums differ; input changed");
            if (i) { raw.push_back(x); wrapped.push_back(y); }
        }
        bench::report("liburing storage", raw);
        bench::report("snowy storage", wrapped);
        std::cout << "mode=" << mode << " depth=" << work.depth << " bytes=" << work.size
                  << " checksum=" << a << " (IOPS = 1e9 / ns/op; cache state uncontrolled)\n";
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
