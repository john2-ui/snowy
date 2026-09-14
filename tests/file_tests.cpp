/** @file file_tests.cpp
 *  @brief Check positional concurrency, EOF, cancellation, and exclusive file creation. */
#include <snowy/snowy.hpp>
#include <array>
#include <cstdlib>
#include <iostream>
#include <random>

/** @brief Fail in all builds. @param ok Required invariant. */
void check(bool ok) { if (!ok) std::abort(); }
/** @brief Own only the temporary directory and file created by this test. */
struct temp {
    std::filesystem::path dir;
    /** @brief Create an exclusive random directory, never reuse existing data. */
    temp() {
        std::random_device random;
        for (int i = 0; i < 100; ++i) {
            auto candidate = std::filesystem::temp_directory_path() /
                ("snowy-" + std::to_string(random()) + "-" + std::to_string(random()));
            if (std::filesystem::create_directory(candidate)) { dir = std::move(candidate); return; }
        }
        throw std::runtime_error("cannot create temporary directory");
    }
    /** @brief Remove only the known test file and its empty directory. */
    ~temp() {
        std::error_code ignored;
        std::filesystem::remove(dir / "data", ignored);
        std::filesystem::remove(dir, ignored);
    }
};
/** @brief Write a unique block. @param file Target. @param block Index. */
snowy::task<> write(snowy::file& file, unsigned block) {
    std::array<std::byte, 4096> data;
    data.fill(static_cast<std::byte>(block));
    co_await snowy::write_all(file, data, std::uint64_t{block} * data.size());
}
/** @brief Read and validate a unique block. @param file Source. @param block Index. */
snowy::task<> read(snowy::file& file, unsigned block) {
    std::array<std::byte, 4096> data;
    check((co_await file.read(data, std::uint64_t{block} * data.size())) == data.size());
    for (auto b : data) check(b == static_cast<std::byte>(block));
}
/** @brief Accept either side of a completion/cancellation race, exactly once.
 *  @param file Source. @param token Shared token. @param done Completion count. */
snowy::task<> race(snowy::file& file, std::stop_token token, unsigned& done) {
    std::array<std::byte, 4096> data;
    try { check((co_await file.read(data, 0, token)) == data.size()); }
    catch (const std::system_error& e) { check(e.code() == std::errc::operation_canceled); }
    ++done;
}
/** @brief Stop after queued readers start. @param loop Owner. @param stop Source. */
snowy::task<> cancel(snowy::loop& loop, std::stop_source& stop) {
    co_await loop.schedule();
    stop.request_stop();
}
/** @brief Exercise concurrent positions and edge cases. @param loop Owner. @param file Target. */
snowy::task<> run(snowy::loop& loop, snowy::file& file) {
    std::vector<snowy::task<>> jobs;
    for (unsigned i = 0; i < 64; ++i) jobs.push_back(write(file, i));
    co_await snowy::when_all(loop, std::move(jobs));
    co_await file.flush();
    jobs.clear();
    for (unsigned i = 0; i < 64; ++i) jobs.push_back(read(file, i));
    co_await snowy::when_all(loop, std::move(jobs));
    std::array<std::byte, 16> data{};
    check((co_await file.read(data, 64 * 4096)) == 0);
    check((co_await file.read(data, 64 * 4096 - 1)) == 1);
    check((co_await file.read({}, 0)) == 0);
    std::stop_source stop;
    stop.request_stop();
    bool caught = false;
    try { co_await file.write(data, 0, stop.get_token()); }
    catch (const std::system_error& e) { caught = e.code() == std::errc::operation_canceled; }
    check(caught);
    caught = false;
    try { co_await file.read(data, UINT64_MAX); }
    catch (const std::invalid_argument&) { caught = true; }
    check(caught);
    co_await read(file, 0);
    std::stop_source concurrent_stop;
    unsigned done = 0;
    jobs.clear();
    for (unsigned i = 0; i < 512; ++i) jobs.push_back(race(file, concurrent_stop.get_token(), done));
    jobs.push_back(cancel(loop, concurrent_stop));
    co_await snowy::when_all(loop, std::move(jobs));
    check(done == 512);
    co_await read(file, 0);
}
/** @brief Observe write failure on a read-only file. @param file Read-only handle. */
snowy::task<> readonly(snowy::file& file) {
    std::array<std::byte, 1> data{};
    bool caught = false;
    try { co_await file.write(data, 0); }
    catch (const std::system_error&) { caught = true; }
    check(caught);
}
/** @brief Run file tests without touching preexisting paths. */
int main() {
    try {
        temp temp;
        snowy::loop loop;
        const auto path = temp.dir / "data";
        {
            snowy::file file(loop, path, snowy::file::mode::create);
            bool caught = false;
            try { snowy::file duplicate(loop, path, snowy::file::mode::create); }
            catch (const std::system_error&) { caught = true; }
            check(caught);
            loop.run(run(loop, file));
        }
        snowy::file file(loop, path);
        loop.run(readonly(file));
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
