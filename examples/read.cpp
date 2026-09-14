/** @file read.cpp
 *  @brief Read an existing file in chunks without modifying it. */
#include <snowy/snowy.hpp>
#include <array>
#include <iostream>

/** @brief Count bytes using explicit file offsets. @param loop Owner. @param path Borrowed path. */
snowy::task<> read(snowy::loop& loop, const char* path) {
    snowy::file file(loop, path);
    std::array<std::byte, 4096> buffer;
    std::uint64_t offset = 0;
    while (auto count = co_await file.read(buffer, offset)) offset += count;
    std::cout << "Read " << offset << " bytes\n";
}
/** @brief Run read-only file I/O. @param argc Argument count. @param argv Existing file path. */
int main(int argc, char** argv) {
    try {
        if (argc != 2) throw std::invalid_argument("usage: snowy_read path");
        snowy::loop loop;
        loop.run(read(loop, argv[1]));
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
