/** @file syscall_tests.cpp
 *  @brief Native filesystem, vectored I/O, splice, poll and child-wait regression tests. */
#include <snowy/uring_ops.hpp>
#include <poll.h>
#include <array>
#include <cstdlib>
#include <iostream>

/** @brief Assert in every configuration. @param ok Required condition. */
void check(bool ok) { if (!ok) std::abort(); }
/** @brief Own only this test's temporary paths. */
struct temp {
    char path[40] = "/tmp/snowy-syscall-XXXXXX";
    std::string first, second, dir;
    /** @brief Create a unique directory without modifying existing paths. */
    temp() {
        if (!mkdtemp(path)) throw std::system_error(errno, std::generic_category());
        first = std::string(path) + "/a";
        second = std::string(path) + "/b";
        dir = std::string(path) + "/dir";
    }
    /** @brief Remove this test's known files, including on an assertion exception. */
    ~temp() { ::unlink(first.c_str()); ::unlink(second.c_str()); ::rmdir(dir.c_str()); ::rmdir(path); }
};
/** @brief Exercise typed borrowed operations. @param loop Owner. */
snowy::task<> run(snowy::loop& loop) {
    temp files;
    auto file = co_await snowy::uring::openat(loop, AT_FDCWD, files.first.c_str(), O_RDWR | O_CREAT | O_EXCL, 0600);
    check((fcntl(file.get(), F_GETFD) & FD_CLOEXEC) != 0);
    std::array<std::byte, 32> data{};
    data.fill(std::byte{42});
    std::array<iovec, 2> vectors{{{data.data(), 16}, {data.data() + 16, 16}}};
    check((co_await snowy::uring::writev(loop, file.get(), vectors, 0)) == 32);
    data.fill(std::byte{});
    check((co_await snowy::uring::readv(loop, file.get(), vectors, 0)) == 32);
    for (auto byte : data) check(byte == std::byte{42});
    co_await snowy::uring::fallocate(loop, file.get(), 0, 4096);
    co_await snowy::uring::fsync(loop, file.get());
    struct statx info{};
    co_await snowy::uring::statx(loop, AT_FDCWD, files.first.c_str(), info);
    check(info.stx_size == 4096);
    co_await snowy::uring::rename(loop, files.first.c_str(), files.second.c_str());
    co_await snowy::uring::mkdir(loop, files.dir.c_str(), 0700);
    co_await snowy::uring::unlink(loop, files.dir.c_str(), AT_REMOVEDIR);
    int pipe[2];
    check(pipe2(pipe, O_CLOEXEC) == 0);
    snowy::uring::fd reader(pipe[0]), writer(pipe[1]);
    check((co_await snowy::uring::splice(loop, file.get(), 0, writer.get(), -1, 32)) == 32);
    check((co_await snowy::uring::poll(loop, reader.get(), POLLIN)) & POLLIN);
    data.fill(std::byte{});
    check(::read(reader.get(), data.data(), data.size()) == 32);
    for (auto byte : data) check(byte == std::byte{42});
    std::stop_source stop;
    loop.post([&] { stop.request_stop(); });
    bool caught = false;
    try { co_await snowy::uring::poll(loop, reader.get(), POLLIN, stop.get_token()); }
    catch (const std::system_error& e) { caught = e.code() == std::errc::operation_canceled; }
    check(caught);
    co_await snowy::uring::unlink(loop, files.second.c_str());
    caught = false;
    try { co_await snowy::uring::openat(loop, AT_FDCWD, files.first.c_str(), O_RDONLY); }
    catch (const std::system_error& e) { caught = e.code().value() == ENOENT; }
    check(caught);
}
/** @brief Observe a child exit through the native completion queue. @param loop Owner. */
snowy::task<> child(snowy::loop& loop) {
    const pid_t pid = fork();
    if (pid < 0) throw std::system_error(errno, std::generic_category());
    if (!pid) _exit(7);
    struct reap {
        pid_t pid;
        /** @brief Reap even if the kernel operation is unsupported. */
        ~reap() { while (::waitpid(pid, nullptr, 0) < 0 && errno == EINTR) {} }
    } reap{pid};
    siginfo_t info{};
    co_await snowy::uring::waitid(loop, P_PID, static_cast<id_t>(pid), info);
    check(info.si_pid == pid && info.si_code == CLD_EXITED && info.si_status == 7);
}
/** @brief Run ordinary operations or the optional child-wait opcode.
 *  @param argc Count. @param argv Optional waitid mode. */
int main(int argc, char** argv) {
    const bool wait = argc > 1 && std::string_view(argv[1]) == "waitid";
    try { snowy::loop loop; if (wait) loop.run(child(loop)); else loop.run(run(loop)); }
    catch (const std::system_error& e) {
        std::cerr << e.what() << '\n';
        if (wait && !std::getenv("SNOWY_REQUIRE_ADVANCED")
            && (e.code().value() == EINVAL || e.code().value() == EOPNOTSUPP || e.code().value() == ENOSYS)) return 77;
        return 1;
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
