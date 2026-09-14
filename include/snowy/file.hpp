/** @file file.hpp
 *  @brief Offset-based regular-file I/O with native Linux/Windows completions. */
#pragma once
#include "snowy/detail/io.hpp"
#include <filesystem>
#include <span>

namespace snowy {
/** @brief Owned regular file; concurrent non-overlapping positional I/O is allowed.
 *  @details File and buffers must outlive operations. Metadata/open/close are
 *  synchronous. macOS uses a fixed two-worker pool for read/write/flush; Windows
 *  uses that pool only for FlushFileBuffers. Cancellation drains in-flight work. */
class file {
public:
    enum class mode { read, read_write, create }; ///< create is exclusive; never truncates.
    /** @brief Open a regular file. @param loop Owner. @param path Native filesystem path.
     *  @param access Access mode. @param direct Bypass the data cache (Linux/Windows).
     *  @details Direct I/O requires device-specific buffer/length/offset alignment;
     *  macOS rejects it instead of substituting a different cache policy.
     *  @throws std::system_error on OS failure or unsupported direct I/O. */
    file(loop& loop, const std::filesystem::path& path, mode access = mode::read, bool direct = false);
    /** @brief Borrow the native handle; never close or change it while owned here. */
    detail::socket_id native_handle() const noexcept { return fd_; }
    /** @brief Move an idle file. @param other Source becoming empty. */
    file(file&& other);
    file(const file&) = delete;
    file& operator=(const file&) = delete;
    /** @brief Close an idle file; pending I/O is a contract violation. */
    ~file();
    /** @brief Read at an explicit offset; zero means EOF for nonempty buffers.
     *  @param buffer Borrowed writable bytes. @param offset Byte position.
     *  @param token Stop token. @return Partial byte count. */
    task<std::size_t> read(std::span<std::byte> buffer, std::uint64_t offset,
                          std::stop_token token = {}) {
        return transfer(buffer.data(), buffer.size(), offset, false, token);
    }
    /** @brief Write at an explicit offset; callers synchronize overlapping writes.
     *  @param buffer Borrowed bytes. @param offset Byte position.
     *  @param token Stop token. @return Partial byte count. */
    task<std::size_t> write(std::span<const std::byte> buffer, std::uint64_t offset,
                           std::stop_token token = {}) {
        return transfer(const_cast<std::byte*>(buffer.data()), buffer.size(), offset, true, token);
    }
    /** @brief Flush OS buffers after awaited writes. @param token Stop token.
     *  @details Does not join concurrent writers; await them before calling. */
    task<> flush(std::stop_token token = {});
private:
    loop* loop_;
    detail::socket_id fd_ = detail::invalid_socket; ///< Windows stores HANDLE bits, not a socket.
    std::size_t active_ = 0;
    /** @brief Reject empty files or calls from the wrong loop thread. */
    void check() const;
    /** @brief Close the native file, never a Winsock handle. */
    void close() noexcept;
    /** @brief Reserve file lifetime for one operation. */
    struct use {
        file& owner;
        /** @brief Increment owner-thread activity. @param f Borrowed file. */
        explicit use(file& f) : owner(f) { f.check(); ++f.active_; }
        /** @brief Release activity only after native/worker completion. */
        ~use() { --owner.active_; }
    };
    /** @brief Perform bounded positional I/O. @param buffer Borrowed bytes.
     *  @param size Requested bytes. @param offset Position. @param write Send direction.
     *  @param token Optional cancellation. */
    task<std::size_t> transfer(void* buffer, std::size_t size, std::uint64_t offset,
                               bool write, std::stop_token token);
};
/** @brief Write all bytes at offset, honoring partial writes.
 *  @param file Borrowed file. @param buffer Borrowed bytes. @param offset Position.
 *  @param token Cancellation token; partial progress is not rolled back. */
task<> write_all(file& file, std::span<const std::byte> buffer, std::uint64_t offset,
                 std::stop_token token = {});
} // namespace snowy
