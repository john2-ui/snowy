/** @file file.cpp
 *  @brief Regular-file ownership, positional I/O, and bounded blocking fallbacks. */
#include "snowy/file.hpp"
#include "snowy/pool.hpp"
#include <climits>
#ifndef _WIN32
#include <fcntl.h>
#include <sys/stat.h>
#endif

namespace snowy {
namespace {
#if defined(__APPLE__) || defined(_WIN32)
/** @brief Share two workers for APIs without a native completion interface. */
pool& workers() { static pool value(2); return value; }
#endif
/** @brief Reject already canceled operations. @param loop Owner. @param token Stop token. */
void canceled(loop& loop, std::stop_token token) {
    if (loop.stopped() || token.stop_requested())
        throw std::system_error(std::make_error_code(std::errc::operation_canceled));
}
}
file::file(loop& loop, const std::filesystem::path& path, mode access, bool direct) : loop_(&loop) {
    loop.check();
    if (path.empty() || path.native().find(std::filesystem::path::value_type{}) != std::filesystem::path::string_type::npos)
        throw std::invalid_argument("invalid file path");
#ifdef _WIN32
    const DWORD rights = access == mode::read ? GENERIC_READ : GENERIC_READ | GENERIC_WRITE;
    auto handle = CreateFileW(path.c_str(), rights, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        nullptr, access == mode::create ? CREATE_NEW : OPEN_EXISTING,
        FILE_FLAG_OVERLAPPED | (direct ? FILE_FLAG_NO_BUFFERING : 0), nullptr);
    if (handle == INVALID_HANDLE_VALUE)
        throw std::system_error(static_cast<int>(GetLastError()), std::system_category(), "CreateFile");
    fd_ = reinterpret_cast<std::uintptr_t>(handle);
#else
    int flags = access == mode::read ? O_RDONLY : O_RDWR;
#ifdef __linux__
    if (direct) flags |= O_DIRECT;
#else
    if (direct) throw std::system_error(std::make_error_code(std::errc::operation_not_supported));
#endif
    if (access == mode::create) flags |= O_CREAT | O_EXCL;
    fd_ = ::open(path.c_str(), flags | O_CLOEXEC | O_NONBLOCK, 0600);
    if (fd_ < 0) throw std::system_error(errno, std::generic_category(), "open");
#endif
    try {
#ifdef _WIN32
        if (GetFileType(handle) != FILE_TYPE_DISK) throw std::invalid_argument("not a regular file");
#else
        struct stat info{};
        if (fstat(fd_, &info)) throw std::system_error(errno, std::generic_category(), "fstat");
        if (!S_ISREG(info.st_mode)) throw std::invalid_argument("not a regular file");
#endif
        loop.attach(static_cast<std::uintptr_t>(fd_));
    } catch (...) { close(); throw; }
}
file::file(file&& other) : loop_(other.loop_) {
    other.check();
    if (other.active_) throw std::logic_error("move of busy file");
    fd_ = std::exchange(other.fd_, detail::invalid_socket);
}
file::~file() { if (active_) std::terminate(); close(); }
void file::close() noexcept {
    if (fd_ == detail::invalid_socket) return;
#ifdef _WIN32
    CloseHandle(reinterpret_cast<HANDLE>(fd_));
#else
    ::close(fd_);
#endif
    fd_ = detail::invalid_socket;
}
void file::check() const {
    loop_->check();
    if (fd_ == detail::invalid_socket) throw std::logic_error("empty file");
}
task<std::size_t> file::transfer(void* buffer, std::size_t size, std::uint64_t offset,
                                bool write, std::stop_token token) {
    use guard(*this);
    canceled(*loop_, token);
    const auto length = static_cast<unsigned>(std::min<std::size_t>(size, INT_MAX));
    if (offset > INT64_MAX || length > INT64_MAX - offset)
        throw std::invalid_argument("file offset overflow");
    if (!length) co_return 0;
#ifdef __APPLE__
    co_return co_await workers().run(*loop_, [=, this] {
        ssize_t count;
        do {
            count = write ? ::pwrite(fd_, buffer, length, static_cast<off_t>(offset))
                          : ::pread(fd_, buffer, length, static_cast<off_t>(offset));
        } while (count < 0 && errno == EINTR);
        if (count < 0) throw std::system_error(errno, std::generic_category());
        return static_cast<std::size_t>(count);
    }, token);
#else
    detail::io op{*loop_, write ? detail::opcode::file_write : detail::opcode::file_read,
                  fd_, buffer, length, token};
    op.offset = offset;
    co_return co_await op;
#endif
}
task<> file::flush(std::stop_token token) {
    use guard(*this);
    canceled(*loop_, token);
#ifdef __linux__
    co_await detail::io{*loop_, detail::opcode::file_sync, fd_, nullptr, 0, token};
#else
    co_await workers().run(*loop_, [this] {
#ifdef _WIN32
        if (!FlushFileBuffers(reinterpret_cast<HANDLE>(fd_)))
            throw std::system_error(static_cast<int>(GetLastError()), std::system_category());
#else
        int r;
        do { r = fsync(fd_); } while (r < 0 && errno == EINTR);
        if (r < 0) throw std::system_error(errno, std::generic_category());
#endif
    }, token);
#endif
}
task<> write_all(file& file, std::span<const std::byte> buffer, std::uint64_t offset,
                 std::stop_token token) {
    while (!buffer.empty()) {
        const auto size = co_await file.write(buffer, offset, token);
        if (!size) throw std::system_error(std::make_error_code(std::errc::io_error));
        offset += size;
        buffer = buffer.subspan(size);
    }
}
} // namespace snowy
