/** @file uring_ops.cpp
 *  @brief Registered-resource ownership and typed native operations. */
#include "snowy/uring.hpp"
#include <bit>
#include <climits>
#include <deque>

// Native SQEs in coroutine frames contain the Linux UAPI zero-length array.
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic ignored "-Wpedantic"
#endif

namespace snowy::uring {
namespace {
/** @brief Convert negative liburing results. @param result Return code. */
void verify(int result) {
    if (result < 0) throw std::system_error(-result, std::generic_category());
}
/** @brief Validate a complete positional request. @param size Bytes. @param offset Position. */
void bounds(std::size_t size, std::uint64_t offset) {
    if (size > INT_MAX || offset > INT64_MAX || size > INT64_MAX - offset)
        throw std::invalid_argument("uring I/O range overflow");
}
/** @brief Prepare ordinary/fixed-buffer positional I/O. @param fd Native fd or fixed index.
 *  @param data Borrowed region. @param offset Position. @param write Direction.
 *  @param slot Registered buffer index, or -1. */
io_uring_sqe transfer(int fd, std::span<std::byte> data, std::uint64_t offset, bool write, int slot = -1) {
    bounds(data.size(), offset);
    io_uring_sqe sqe{};
    if (slot >= 0) {
        if (write) io_uring_prep_write_fixed(&sqe, fd, data.data(), static_cast<unsigned>(data.size()), offset, slot);
        else io_uring_prep_read_fixed(&sqe, fd, data.data(), static_cast<unsigned>(data.size()), offset, slot);
    } else {
        if (write) io_uring_prep_write(&sqe, fd, data.data(), static_cast<unsigned>(data.size()), offset);
        else io_uring_prep_read(&sqe, fd, data.data(), static_cast<unsigned>(data.size()), offset);
    }
    return sqe;
}
}

memory::memory(std::size_t size, std::size_t alignment) : size_(size), alignment_(alignment) {
    if (!size || alignment < alignof(std::max_align_t) || !std::has_single_bit(alignment))
        throw std::invalid_argument("invalid aligned allocation");
    data_ = static_cast<std::byte*>(::operator new(size, std::align_val_t{alignment}));
}
memory::~memory() { ::operator delete(data_, std::align_val_t{alignment_}); }

op::op(loop& loop, io_uring_sqe sqe, std::stop_token token, files* f, buffers* b, bool* direction)
    : detail::io(loop, detail::opcode::native, -1, nullptr, 0, token, direction), sqe_(sqe), files_(f), buffers_(b) {
    loop.check();
    if (sqe.flags & (IOSQE_CQE_SKIP_SUCCESS | IOSQE_IO_LINK | IOSQE_IO_HARDLINK))
        throw std::invalid_argument("standalone operation cannot skip or link completions");
    if ((f && &f->loop_ != &loop) || (b && &b->loop_ != &loop))
        throw std::invalid_argument("registered resource belongs to another loop");
    if (f) ++f->active_;
    if (b) ++b->active_;
    prepare = [](detail::io& base, io_uring_sqe& entry) noexcept { entry = static_cast<op&>(base).sqe_; };
    result = [](detail::io& base, int res, unsigned flags) noexcept {
        // A zero-copy notification releases memory; it is not a byte count.
        if (flags & IORING_CQE_F_NOTIF) return;
        if (res < 0) base.error = {-res, std::generic_category()};
        else base.bytes = static_cast<std::size_t>(res);
    };
}
op::~op() {
    if (active) std::terminate();
    if (files_) --files_->active_;
    if (buffers_) --buffers_->active_;
}

files::files(loop& loop, std::span<const int> fds) : loop_(loop), size_(fds.size()) {
    if (fds.empty() || fds.size() > INT_MAX) throw std::invalid_argument("invalid file table size");
    verify(io_uring_register_files(&access::ring(loop), fds.data(), static_cast<unsigned>(fds.size())));
}
files::~files() {
    if (active_ || io_uring_unregister_files(&access::ring(loop_)) < 0) std::terminate();
}
void files::check(unsigned index) const {
    loop_.check();
    if (index >= size_) throw std::out_of_range("file table index");
}
op files::read(unsigned index, std::span<std::byte> data, std::uint64_t offset, std::stop_token token) {
    check(index);
    auto sqe = transfer(static_cast<int>(index), data, offset, false);
    sqe.flags |= IOSQE_FIXED_FILE;
    return {loop_, sqe, token, this};
}
op files::write(unsigned index, std::span<const std::byte> data, std::uint64_t offset, std::stop_token token) {
    check(index);
    auto sqe = transfer(static_cast<int>(index), {const_cast<std::byte*>(data.data()), data.size()}, offset, true);
    sqe.flags |= IOSQE_FIXED_FILE;
    return {loop_, sqe, token, this};
}
op files::read(unsigned index, buffers& table, unsigned slot, std::uint64_t offset, std::stop_token token) {
    check(index);
    auto sqe = transfer(static_cast<int>(index), table.at(slot), offset, false, static_cast<int>(slot));
    sqe.flags |= IOSQE_FIXED_FILE;
    return {loop_, sqe, token, this, &table};
}
op files::write(unsigned index, buffers& table, unsigned slot, std::uint64_t offset, std::stop_token token) {
    check(index);
    auto sqe = transfer(static_cast<int>(index), table.at(slot), offset, true, static_cast<int>(slot));
    sqe.flags |= IOSQE_FIXED_FILE;
    return {loop_, sqe, token, this, &table};
}
buffers::buffers(loop& loop, std::span<const iovec> regions) : loop_(loop), regions_(regions.begin(), regions.end()) {
    if (regions.empty() || regions.size() > 65536) throw std::invalid_argument("invalid buffer table size");
    for (auto region : regions) if (!region.iov_base || !region.iov_len || region.iov_len > INT_MAX)
        throw std::invalid_argument("invalid registered buffer");
    verify(io_uring_register_buffers(&access::ring(loop), regions_.data(), static_cast<unsigned>(regions_.size())));
}
buffers::~buffers() {
    if (active_ || io_uring_unregister_buffers(&access::ring(loop_)) < 0) std::terminate();
}
std::span<std::byte> buffers::at(unsigned index) const {
    loop_.check();
    const auto& region = regions_.at(index);
    return {static_cast<std::byte*>(region.iov_base), region.iov_len};
}
bool supports(loop& loop, unsigned opcode) {
    auto* probe = io_uring_get_probe_ring(&access::ring(loop));
    if (!probe) throw std::system_error(errno ? errno : ENOMEM, std::generic_category(), "io_uring probe");
    const bool result = io_uring_opcode_supported(probe, static_cast<int>(opcode));
    io_uring_free_probe(probe);
    return result;
}
op read(loop& loop, int fd, std::span<std::byte> data, std::uint64_t offset, std::stop_token token) {
    return {loop, transfer(fd, data, offset, false), token};
}
op write(loop& loop, int fd, std::span<const std::byte> data, std::uint64_t offset, std::stop_token token) {
    return {loop, transfer(fd, {const_cast<std::byte*>(data.data()), data.size()}, offset, true), token};
}
op nop(loop& loop, std::stop_token token) {
    io_uring_sqe sqe{};
    io_uring_prep_nop(&sqe);
    return {loop, sqe, token};
}

task<std::vector<int>> submit(loop& loop, std::span<const io_uring_sqe> entries,
                             link order, std::stop_token token) {
    loop.check();
    if (loop.stopped() || token.stop_requested())
        throw std::system_error(std::make_error_code(std::errc::operation_canceled));
    if (entries.empty()) co_return std::vector<int>{};
    if (entries.size() > 32768) throw std::invalid_argument("batch exceeds maximum ring size");
    // Allocate stable nodes and result slots before any SQE can reach the kernel.
    event done(loop);
    std::size_t pending = entries.size();
    std::vector<int> results(entries.size());
    struct entry : detail::io {
        io_uring_sqe sqe;
        int& output;
        std::size_t& pending;
        event& done;
        /** @brief Bind a stable batch node. @param loop Owner. @param sqe Prepared SQE.
         *  @param output Result slot. @param pending Remaining count. @param done Barrier. */
        entry(snowy::loop& loop, io_uring_sqe sqe, int& output, std::size_t& pending, event& done)
            : detail::io(loop, detail::opcode::native), sqe(sqe), output(output), pending(pending), done(done) {
            prepare = [](detail::io& op, io_uring_sqe& sqe) noexcept { sqe = static_cast<entry&>(op).sqe; };
            result = [](detail::io& op, int res, unsigned) noexcept { static_cast<entry&>(op).output = res; };
            retire = [](detail::io& op) noexcept {
                auto& self = static_cast<entry&>(op);
                if (!--self.pending) self.done.set();
            };
        }
    };
    std::deque<entry> requests;
    bool drain = false;
    for (std::size_t i = 0; i < entries.size(); ++i) {
        auto sqe = entries[i];
        switch (sqe.opcode) {
        case IORING_OP_NOP: case IORING_OP_READ: case IORING_OP_WRITE:
        case IORING_OP_READV: case IORING_OP_WRITEV: case IORING_OP_READ_FIXED:
        case IORING_OP_WRITE_FIXED: case IORING_OP_FSYNC: case IORING_OP_FALLOCATE:
        case IORING_OP_SPLICE: break;
        default: throw std::invalid_argument("batch requires supported single-shot operations");
        }
        if (sqe.flags & (IOSQE_CQE_SKIP_SUCCESS | IOSQE_BUFFER_SELECT | IOSQE_IO_LINK | IOSQE_IO_HARDLINK))
            throw std::invalid_argument("invalid batch SQE flags");
        if (i + 1 < entries.size()) {
            if (order == link::soft) sqe.flags |= IOSQE_IO_LINK;
            if (order == link::hard) sqe.flags |= IOSQE_IO_HARDLINK;
        }
        drain |= (sqe.flags & IOSQE_IO_DRAIN) != 0;
        requests.emplace_back(loop, sqe, results[i], pending, done);
    }
    access::reserve(loop, static_cast<unsigned>(entries.size()), drain);
    // Register callbacks before publishing. A racing stop only marks nodes; the
    // owner processes cancellation after this complete chain is in the SQ.
    for (auto& request : requests)
        if (token.stop_possible()) request.callback.emplace(token, detail::op::cancel_fn{&request});
    for (auto& request : requests) access::start(request);
    co_await done.join();
    co_return results;
}
} // namespace snowy::uring
