/** @file resource_tests.cpp
 *  @brief Registered-slot updates, sparse tables and direct descriptor lifetimes. */
#include <snowy/uring.hpp>
#include <snowy/when.hpp>
#include <array>
#include <cstdlib>
#include <iostream>

/** @brief Assert with a source line in every build. @param ok Condition. @param line Location. */
void verify(bool ok, int line) { if (!ok) { std::cerr << "line " << line << '\n'; std::abort(); } }
#define check(...) verify((__VA_ARGS__), __LINE__)
/** @brief Own a unique temporary file. */
struct temp {
    char path[40] = "/tmp/snowy-resource-XXXXXX";
    int fd = mkstemp(path);
    /** @brief Reject failed creation. */
    temp() { if (fd < 0) throw std::system_error(errno, std::generic_category()); }
    /** @brief Remove only this test's file. */
    ~temp() { ::close(fd); ::unlink(path); }
};
/** @brief Exercise mutable tables. @param loop Owner. @param file Existing file.
 *  @param sparse Start with kernel sparse registrations. */
snowy::task<> tables(snowy::loop& loop, temp& file, bool sparse) {
    snowy::uring::memory memory(8192);
    auto data = memory.bytes();
    std::fill(data.begin(), data.end(), std::byte{42});
    std::array<int, 2> fds{file.fd, -1};
    std::array<iovec, 2> regions{{{data.data(), 4096}, {data.data() + 4096, 4096}}};
    auto files = sparse ? std::make_unique<snowy::uring::files>(loop, 2u)
                        : std::make_unique<snowy::uring::files>(loop, fds);
    auto buffers = sparse ? std::make_unique<snowy::uring::buffers>(loop, 2u)
                          : std::make_unique<snowy::uring::buffers>(loop, regions);
    check(files->update(0, fds) == 2);
    check(buffers->update(0, regions) == 2);
    check((co_await files->write(0, *buffers, 0, 0)) == 4096);
    {
        auto pin = files->read(0, *buffers, 1, 0);
        bool rejected = false;
        try { files->update(0, fds); } catch (const std::logic_error&) { rejected = true; }
        check(rejected);
        rejected = false;
        try { buffers->update(0, regions); } catch (const std::logic_error&) { rejected = true; }
        check(rejected);
    }
    fds = {-1, file.fd};
    check(files->update(0, fds) == 2);
    std::fill(data.begin(), data.end(), std::byte{});
    check((co_await files->read(1, *buffers, 1, 0)) == 4096);
    for (auto b : data.subspan(4096)) check(b == std::byte{42});
    auto reads = co_await snowy::when_all(loop,
        [&] { return files->read(1, *buffers, 0, 0); },
        [&] { return files->read(1, *buffers, 1, 0); });
    check(std::get<0>(reads) == 4096 && std::get<1>(reads) == 4096);
    bool caught = false;
    try { co_await files->read(0, *buffers, 0, 0); }
    catch (const std::system_error& e) { caught = e.code().value() == EBADF; }
    check(caught);
    regions[0] = {};
    check(buffers->update(0, std::span{regions}.first(1)) == 1);
    caught = false;
    try { buffers->at(0); } catch (const std::logic_error&) { caught = true; }
    check(caught);
    check(files->update(2, {}) == 0 && buffers->update(2, {}) == 0);
}
/** @brief Open/write/read/close without ordinary descriptor allocation.
 *  @param loop Owner. @param file Existing path. */
snowy::task<> direct(snowy::loop& loop, temp& file) {
    std::array<int, 2> empty{-1, -1};
    snowy::uring::files files(loop, empty);
    std::array<std::byte, 4> data{std::byte{42}};
    for (int i = 0; i < 64; ++i) {
        check((co_await files.open(0, file.path, O_RDWR)) == 0);
        check((co_await files.write(0, data, 0)) == data.size());
        check((co_await files.read(0, data, 0)) == data.size());
        check(data[0] == std::byte{42});
        check((co_await files.close(0)) == 0);
    }
    std::stop_source stop;
    stop.request_stop();
    bool caught = false;
    try { co_await files.open(0, file.path, O_RDONLY, 0, stop.get_token()); }
    catch (const std::system_error& e) { caught = e.code() == std::errc::operation_canceled; }
    check(caught);
}
/** @brief Echo using a direct accepted slot. @param files Table. @param listener Ordinary listener. */
snowy::task<> server(snowy::uring::files& files, snowy::socket& listener) {
    check((co_await files.accept(0, listener)) == 0);
    std::array<std::byte, 1> data{};
    check((co_await files.recv(0, data)) == 1 && data[0] == std::byte{42});
    check((co_await files.send(0, data)) == 1);
    co_await files.close(0);
}
/** @brief Talk to the direct accepted socket. @param loop Owner. @param address Listener endpoint. */
snowy::task<> client(snowy::loop& loop, snowy::endpoint address) {
    auto socket = co_await snowy::socket::connect(loop, address);
    std::array<std::byte, 1> data{std::byte{42}};
    co_await snowy::write_all(socket, data);
    check((co_await socket.read(data)) == 1 && data[0] == std::byte{42});
}
/** @brief Exercise direct socket creation and accept. @param loop Owner. */
snowy::task<> network(snowy::loop& loop) {
    std::array<int, 2> empty{-1, -1};
    snowy::uring::files files(loop, empty);
    co_await files.socket(0, AF_INET, SOCK_STREAM);
    co_await files.close(0);
    auto listener = snowy::socket::listen(loop, {"127.0.0.1", 0});
    co_await snowy::when_all(loop, server(files, listener), client(loop, listener.local()));
}
/** @brief Run one capability; only optional kernel modes can skip.
 *  @param argc Argument count. @param argv tables, sparse, directfd or fixednet. */
int main(int argc, char** argv) {
    const std::string mode = argc > 1 ? argv[1] : "tables";
    try {
        snowy::loop loop;
        temp file;
        if (mode == "directfd") loop.run(direct(loop, file));
        else if (mode == "fixednet") loop.run(network(loop));
        else loop.run(tables(loop, file, mode == "sparse"));
    } catch (const std::system_error& e) {
        std::cerr << e.what() << '\n';
        if (mode != "tables" && !std::getenv("SNOWY_REQUIRE_ADVANCED")
            && (e.code().value() == EINVAL || e.code().value() == EOPNOTSUPP || e.code().value() == ENOSYS)) return 77;
        return 1;
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
