/** @file uring.hpp
 *  @brief Explicit Linux optimizations; borrowed resources live through completion.
 *  @details Requires liburing 2.3+. Unsupported kernel/device features throw;
 *  optimized requests never silently fall back to another I/O mode. */
#pragma once
#include "snowy/detail/io.hpp"
#include <liburing.h>
#include <span>
#include <new>

namespace snowy::uring {
/** @brief Owner-thread ring configuration, fixed for the lifetime of a loop. */
struct options {
    unsigned entries = 256; ///< SQ entries, 1..32768; kernel rounds up.
    unsigned cq_entries = 0; ///< Zero selects the kernel default.
    unsigned flags = 0; ///< IOPOLL, SQPOLL, SQ_AFF, SINGLE_ISSUER, COOP/DEFER_TASKRUN, TASKRUN_FLAG, SUBMIT_ALL.
    unsigned idle_ms = 1000; ///< SQPOLL idle timeout.
    unsigned cpu = 0; ///< SQ_AFF CPU index, ignored without SQ_AFF.
    unsigned budget = 64; ///< Positive ready-work budget between polls.
};

/** @brief Internal bridge; applications use the typed operations below. */
class access {
public:
    /** @brief Borrow an owner-thread ring. @param loop Owner. */
    static io_uring& ring(loop& loop);
};

/** @brief Owned aligned storage for direct/registered I/O; never resizes. */
class memory {
public:
    /** @brief Allocate uninitialized bytes. @param size Positive byte count.
     *  @param alignment Power of two, at least alignof(max_align_t). */
    explicit memory(std::size_t size, std::size_t alignment = 4096);
    /** @brief Release after every kernel reference and registration is retired. */
    ~memory();
    memory(const memory&) = delete;
    memory& operator=(const memory&) = delete;
    /** @brief Borrow all writable bytes. */
    std::span<std::byte> bytes() const noexcept { return {data_, size_}; }
private:
    std::byte* data_;
    std::size_t size_, alignment_;
};

class files;
class buffers;
/** @brief Coroutine-owned native request; one-shot or zero-copy notification aware.
 *  @details Do not destroy while awaiting. Registered tables and borrowed buffers
 *  outlive this object, including an unawaited request. */
class op : public detail::io {
public:
    /** @brief Bind a prepared operation. @param loop Owner. @param sqe SQE snapshot.
     *  @param token Cancellation token. @param f Optional file-table lifetime pin.
     *  @param b Optional buffer-table lifetime pin. */
    op(loop& loop, io_uring_sqe sqe, std::stop_token token = {}, files* f = nullptr, buffers* b = nullptr);
    /** @brief Release table pins after all completions, including cancel CQEs. */
    ~op();
private:
    io_uring_sqe sqe_;
    files* files_;
    buffers* buffers_;
};

/** @brief One registered file table per loop; registration retains kernel file refs. */
class files {
public:
    /** @brief Register descriptors. @param loop Owner. @param fds Valid native descriptors. */
    files(loop& loop, std::span<const int> fds);
    /** @brief Unregister an idle table; destroying a table referenced by ops terminates. */
    ~files();
    files(const files&) = delete;
    files& operator=(const files&) = delete;
    /** @brief Read using a fixed file index. @param index Registered file slot.
     *  @param data Borrowed destination. @param offset Byte offset. @param token Cancellation. */
    [[nodiscard]] op read(unsigned index, std::span<std::byte> data, std::uint64_t offset, std::stop_token token = {});
    /** @brief Write using a fixed file index. @param index Registered file slot.
     *  @param data Borrowed source. @param offset Byte offset. @param token Cancellation. */
    [[nodiscard]] op write(unsigned index, std::span<const std::byte> data, std::uint64_t offset, std::stop_token token = {});
    /** @brief Read using fixed file and buffer indices. @param index File slot.
     *  @param table Buffer table. @param slot Buffer slot. @param offset Byte offset.
     *  @param token Cancellation. */
    [[nodiscard]] op read(unsigned index, buffers& table, unsigned slot, std::uint64_t offset, std::stop_token token = {});
    /** @brief Write using fixed file and buffer indices. @param index File slot.
     *  @param table Buffer table. @param slot Buffer slot. @param offset Byte offset.
     *  @param token Cancellation. */
    [[nodiscard]] op write(unsigned index, buffers& table, unsigned slot, std::uint64_t offset, std::stop_token token = {});
private:
    friend class op;
    loop& loop_;
    std::size_t size_, active_ = 0;
    /** @brief Validate thread and index. @param index File slot. */
    void check(unsigned index) const;
};

/** @brief One registered buffer table per loop; memory is borrowed, not owned. */
class buffers {
public:
    /** @brief Register stable regions. @param loop Owner. @param regions Writable buffers. */
    buffers(loop& loop, std::span<const iovec> regions);
    /** @brief Unregister before releasing memory, after all operations are destroyed. */
    ~buffers();
    buffers(const buffers&) = delete;
    buffers& operator=(const buffers&) = delete;
    /** @brief Borrow a registered region. @param index Buffer slot. */
    std::span<std::byte> at(unsigned index) const;
private:
    friend class op;
    friend class files;
    loop& loop_;
    std::vector<iovec> regions_;
    std::size_t active_ = 0;
};

/** @brief Probe an opcode, not its flags or hardware prerequisites.
 *  @param loop Owner. @param opcode IORING_OP_* value. */
bool supports(loop& loop, unsigned opcode);
/** @brief Read borrowed memory from a native fd. @param loop Owner. @param fd File.
 *  @param data Destination. @param offset Position. @param token Cancellation. */
[[nodiscard]] op read(loop& loop, int fd, std::span<std::byte> data, std::uint64_t offset, std::stop_token token = {});
/** @brief Write borrowed memory to a native fd. @param loop Owner. @param fd File.
 *  @param data Source. @param offset Position. @param token Cancellation. */
[[nodiscard]] op write(loop& loop, int fd, std::span<const std::byte> data, std::uint64_t offset, std::stop_token token = {});
/** @brief Allocation-free native NOP. @param loop Owner. @param token Cancellation. */
[[nodiscard]] op nop(loop& loop, std::stop_token token = {});
} // namespace snowy::uring
