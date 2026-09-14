/** @file uring_extra.cpp
 *  @brief Thin typed syscall preparation with cancellation-safe fd ownership. */
#include "snowy/uring_ops.hpp"
#include <climits>

namespace snowy::uring {
namespace {
/** @brief Reject null paths before submission. @param path Borrowed path. */
void path(const char* path) { if (!path) throw std::invalid_argument("null path"); }
/** @brief Check vector count, total length and offset. @param data Borrowed vectors.
 *  @param offset Position. */
void vectors(std::span<const iovec> data, std::uint64_t offset) {
    if (data.size() > IOV_MAX || offset > INT64_MAX) throw std::invalid_argument("invalid vector range");
    std::uint64_t total = 0;
    for (auto v : data) {
        if (v.iov_len > INT_MAX - total || (!v.iov_base && v.iov_len))
            throw std::invalid_argument("invalid I/O vector");
        total += v.iov_len;
    }
    if (total > INT64_MAX - offset) throw std::invalid_argument("vector offset overflow");
}
}
open_op::open_op(loop& loop, io_uring_sqe sqe, std::stop_token token) : op(loop, sqe, token) {
    result = [](detail::io& self, int res, unsigned) noexcept {
        if (res < 0) self.error = {-res, std::generic_category()};
        else self.accepted = res;
    };
}
open_op openat(loop& loop, int dir, const char* name, int flags, unsigned mode, std::stop_token token) {
    path(name);
    io_uring_sqe sqe{};
    io_uring_prep_openat(&sqe, dir, name, flags | O_CLOEXEC, mode);
    return {loop, sqe, token};
}
op statx(loop& loop, int dir, const char* name, struct statx& out, unsigned mask, int flags, std::stop_token token) {
    path(name);
    io_uring_sqe sqe{};
    io_uring_prep_statx(&sqe, dir, name, flags, mask, &out);
    return {loop, sqe, token};
}
op rename(loop& loop, const char* from, const char* to, unsigned flags, std::stop_token token) {
    path(from); path(to);
    io_uring_sqe sqe{};
    io_uring_prep_renameat(&sqe, AT_FDCWD, from, AT_FDCWD, to, flags);
    return {loop, sqe, token};
}
op unlink(loop& loop, const char* name, int flags, std::stop_token token) {
    path(name);
    io_uring_sqe sqe{};
    io_uring_prep_unlink(&sqe, name, flags);
    return {loop, sqe, token};
}
op mkdir(loop& loop, const char* name, unsigned mode, std::stop_token token) {
    path(name);
    io_uring_sqe sqe{};
    io_uring_prep_mkdir(&sqe, name, mode);
    return {loop, sqe, token};
}
op fsync(loop& loop, int file, bool data_only, std::stop_token token) {
    io_uring_sqe sqe{};
    io_uring_prep_fsync(&sqe, file, data_only ? IORING_FSYNC_DATASYNC : 0);
    return {loop, sqe, token};
}
op fallocate(loop& loop, int file, std::uint64_t offset, std::uint64_t length, int mode, std::stop_token token) {
    if (offset > INT64_MAX || length > INT64_MAX - offset) throw std::invalid_argument("extent overflow");
    io_uring_sqe sqe{};
    io_uring_prep_fallocate(&sqe, file, mode, offset, length);
    return {loop, sqe, token};
}
op readv(loop& loop, int file, std::span<const iovec> data, std::uint64_t offset, std::stop_token token) {
    vectors(data, offset);
    io_uring_sqe sqe{};
    io_uring_prep_readv(&sqe, file, data.data(), static_cast<unsigned>(data.size()), offset);
    return {loop, sqe, token};
}
op writev(loop& loop, int file, std::span<const iovec> data, std::uint64_t offset, std::stop_token token) {
    vectors(data, offset);
    io_uring_sqe sqe{};
    io_uring_prep_writev(&sqe, file, data.data(), static_cast<unsigned>(data.size()), offset);
    return {loop, sqe, token};
}
op splice(loop& loop, int in, std::int64_t in_offset, int out, std::int64_t out_offset,
          unsigned size, unsigned flags, std::stop_token token) {
    if (size > INT_MAX || in_offset < -1 || out_offset < -1) throw std::invalid_argument("invalid splice range");
    io_uring_sqe sqe{};
    io_uring_prep_splice(&sqe, in, in_offset, out, out_offset, size, flags);
    return {loop, sqe, token};
}
op waitid(loop& loop, idtype_t type, id_t id, siginfo_t& out, int options, std::stop_token token) {
#if defined(IO_URING_VERSION_MAJOR) && (IO_URING_VERSION_MAJOR > 2 || IO_URING_VERSION_MINOR >= 6)
    io_uring_sqe sqe{};
    io_uring_prep_waitid(&sqe, type, id, &out, options, 0);
    return {loop, sqe, token};
#else
    (void)loop; (void)type; (void)id; (void)out; (void)options; (void)token;
    throw std::system_error(std::make_error_code(std::errc::operation_not_supported), "waitid needs liburing 2.6+");
#endif
}
op poll(loop& loop, int file, unsigned events, std::stop_token token) {
    io_uring_sqe sqe{};
    io_uring_prep_poll_add(&sqe, file, events);
    return {loop, sqe, token};
}
} // namespace snowy::uring
