/** @file fixed.cpp
 *  @brief Read an existing file using registered io_uring files and buffers. */
#include <snowy/file.hpp>
#include <snowy/uring.hpp>
#include <iostream>

/** @brief Keep registrations and their memory alive until completion.
 *  @param loop Owner. @param path Existing readable file. */
snowy::task<> read(snowy::loop& loop, const char* path) {
    snowy::file file(loop, path);
    snowy::uring::memory memory(4096);
    const int fd = file.native_handle();
    snowy::uring::files files(loop, std::span(&fd, 1));
    const iovec region{memory.bytes().data(), memory.bytes().size()};
    snowy::uring::buffers buffers(loop, std::span(&region, 1));
    const auto count = co_await files.read(0, buffers, 0, 0);
    std::cout << "Read " << count << " bytes\n";
}

/** @brief Drive the native read. @param argc Argument count. @param argv Existing file path. */
int main(int argc, char** argv) {
    try {
        if (argc != 2) throw std::invalid_argument("usage: snowy_fixed path");
        snowy::loop loop;
        loop.run(read(loop, argv[1]));
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
