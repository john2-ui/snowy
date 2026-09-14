/** @file file.cpp
 *  @brief Read-only positional file benchmark with deterministic access patterns. */
#include "common.hpp"
#include <snowy/snowy.hpp>

/** @brief Issue one lane of non-overlapping sequential or seeded random reads.
 *  @param file Source. @param data Reused buffer. @param count Operations.
 *  @param lane Lane index. @param depth Concurrent lanes. @param blocks File blocks.
 *  @param random Whether to randomize offsets. @param sum Observable checksum. */
snowy::task<> read(snowy::file& file, std::vector<std::byte>& data, unsigned count,
    unsigned lane, unsigned depth, std::uint64_t blocks, bool random, std::uint64_t& sum) {
    std::uint64_t seed = lane + 1;
    for (unsigned i = 0; i < count; ++i) {
        seed ^= seed << 13; seed ^= seed >> 7; seed ^= seed << 17;
        const auto block = (random ? seed : std::uint64_t{i} * depth + lane) % blocks;
        if ((co_await file.read(data, block * data.size())) != data.size())
            throw std::runtime_error("short file read: file changed during benchmark");
        sum += std::to_integer<unsigned>(data.front());
    }
}
/** @brief Measure an existing file without modifying or deleting it.
 *  @param argc Argument count. @param argv Path, per-lane count, depth, pattern, block bytes. */
int main(int argc, char** argv) {
    try {
        if (argc < 2 || argc > 6)
            throw std::invalid_argument("usage: snowy_file path [count/lane] [depth] [random|seq] [bytes]");
        const auto count = argc > 2 ? bench::count(argv[2]) : 10000u;
        const auto depth = argc > 3 ? bench::count(argv[3], 128) : 32u;
        const std::string_view pattern = argc > 4 ? argv[4] : "random";
        const auto size = argc > 5 ? bench::count(argv[5], 16 * 1024 * 1024) : 4096u;
        if (pattern != "random" && pattern != "seq") throw std::invalid_argument("unknown access pattern");
        const auto blocks = std::filesystem::file_size(argv[1]) / size;
        if (!blocks) throw std::invalid_argument("file smaller than one block");
        std::vector<std::vector<std::byte>> data(depth, std::vector<std::byte>(size));
        std::vector<double> samples;
        std::uint64_t sum = 0;
        for (unsigned sample = 0; sample < 10; ++sample) {
            snowy::loop loop;
            snowy::file file(loop, argv[1]);
            for (unsigned lane = 0; lane < depth; ++lane)
                loop.spawn(read(file, data[lane], count, lane, depth, blocks, pattern == "random", sum));
            const auto start = bench::clock::now();
            loop.run();
            if (sample) samples.push_back(bench::elapsed(start) / (static_cast<double>(count) * depth));
        }
        bench::report("file", samples);
        std::cout << "pattern=" << pattern << " depth=" << depth << " bytes=" << size
                  << " checksum=" << sum << " (buffered I/O; cache state uncontrolled)\n";
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
