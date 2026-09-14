/** @file uring_ops.hpp
 *  @brief Typed Linux syscalls; paths, vectors and output structures are borrowed
 *  and must survive the await. Returned descriptors have RAII ownership. */
#pragma once
#include "snowy/uring.hpp"
#include <sys/wait.h>
#include <linux/stat.h>

namespace snowy::uring {
/** @brief Move-only ordinary descriptor; not a registered/direct slot. */
class fd {
    int value_;
public:
    /** @brief Adopt a descriptor. @param value Owned fd or -1. */
    explicit fd(int value = -1) noexcept : value_(value) {}
    fd(const fd&) = delete;
    fd& operator=(const fd&) = delete;
    /** @brief Transfer ownership. @param other Consumed owner. */
    fd(fd&& other) noexcept : value_(other.release()) {}
    /** @brief Close synchronously after all borrowed operations finish. */
    ~fd() { if (value_ >= 0) ::close(value_); }
    /** @brief Borrow the ordinary descriptor. */
    int get() const noexcept { return value_; }
    /** @brief Transfer ownership to the caller. */
    int release() noexcept { return std::exchange(value_, -1); }
};
/** @brief Open request retaining descriptor ownership until await_resume. */
class open_op : public op {
public:
    /** @brief Bind a prepared open operation. @param loop Owner. @param sqe OPENAT SQE.
     *  @param token Cancellation. */
    open_op(loop& loop, io_uring_sqe sqe, std::stop_token token);
    /** @brief Transfer the successfully opened descriptor. */
    uring::fd await_resume() { detail::op::await_resume(); return uring::fd{std::exchange(accepted, -1)}; }
};
/** @brief Open relative to a directory. @param loop Owner. @param dir Directory or AT_FDCWD.
 *  @param path Borrowed path. @param flags O_*. @param mode Creation permissions. @param token Cancellation. */
[[nodiscard]] open_op openat(loop& loop, int dir, const char* path, int flags, unsigned mode = 0, std::stop_token token = {});
/** @brief Query metadata. @param loop Owner. @param dir Directory or AT_FDCWD. @param path Borrowed path.
 *  @param out Borrowed result. @param mask STATX_*. @param flags AT_*. @param token Cancellation. */
[[nodiscard]] op statx(loop& loop, int dir, const char* path, struct statx& out, unsigned mask = STATX_BASIC_STATS,
                     int flags = 0, std::stop_token token = {});
/** @brief Rename a path. @param loop Owner. @param from Borrowed old path. @param to Borrowed new path.
 *  @param flags RENAME_*. @param token Cancellation. */
[[nodiscard]] op rename(loop& loop, const char* from, const char* to, unsigned flags = 0, std::stop_token token = {});
/** @brief Unlink a path or empty directory. @param loop Owner. @param path Borrowed path.
 *  @param flags AT_REMOVEDIR or zero. @param token Cancellation. */
[[nodiscard]] op unlink(loop& loop, const char* path, int flags = 0, std::stop_token token = {});
/** @brief Create a directory. @param loop Owner. @param path Borrowed path.
 *  @param mode Permissions. @param token Cancellation. */
[[nodiscard]] op mkdir(loop& loop, const char* path, unsigned mode = 0755, std::stop_token token = {});
/** @brief Flush a descriptor. @param loop Owner. @param file Ordinary fd.
 *  @param data_only Flush data only. @param token Cancellation. */
[[nodiscard]] op fsync(loop& loop, int file, bool data_only = false, std::stop_token token = {});
/** @brief Allocate/deallocate file extents. @param loop Owner. @param file Ordinary fd.
 *  @param offset First byte. @param length Bytes. @param mode FALLOC_FL_*. @param token Cancellation. */
[[nodiscard]] op fallocate(loop& loop, int file, std::uint64_t offset, std::uint64_t length,
                         int mode = 0, std::stop_token token = {});
/** @brief Scatter read. @param loop Owner. @param file Ordinary fd. @param data Borrowed vectors.
 *  @param offset File position. @param token Cancellation. */
[[nodiscard]] op readv(loop& loop, int file, std::span<const iovec> data, std::uint64_t offset, std::stop_token token = {});
/** @brief Gather write. @param loop Owner. @param file Ordinary fd. @param data Borrowed vectors.
 *  @param offset File position. @param token Cancellation. */
[[nodiscard]] op writev(loop& loop, int file, std::span<const iovec> data, std::uint64_t offset, std::stop_token token = {});
/** @brief Transfer through a pipe without a userspace copy. @param loop Owner. @param in Input fd.
 *  @param in_offset Input position, or -1 for a pipe. @param out Output fd.
 *  @param out_offset Output position, or -1 for a pipe. @param size Bytes. @param flags SPLICE_F_*.
 *  @param token Cancellation. */
[[nodiscard]] op splice(loop& loop, int in, std::int64_t in_offset, int out, std::int64_t out_offset,
                       unsigned size, unsigned flags = 0, std::stop_token token = {});
/** @brief Await child status without blocking the loop (liburing 2.6+, Linux 6.7+).
 *  @param loop Owner. @param type P_PID/P_ALL/P_PGID/P_PIDFD. @param id Selected child.
 *  @param out Borrowed status. @param options WEXITED/WNOWAIT/etc. @param token Cancellation. */
[[nodiscard]] op waitid(loop& loop, idtype_t type, id_t id, siginfo_t& out, int options = WEXITED, std::stop_token token = {});
/** @brief Poll an ordinary descriptor once. @param loop Owner. @param file Borrowed fd.
 *  @param events POLL*. @param token Cancellation. @return Ready event mask. */
[[nodiscard]] op poll(loop& loop, int file, unsigned events, std::stop_token token = {});
} // namespace snowy::uring
