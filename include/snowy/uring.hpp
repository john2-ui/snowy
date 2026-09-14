/** @file uring.hpp
 *  @brief Explicit Linux optimizations; borrowed resources live through completion.
 *  @details Requires liburing 2.3+. Unsupported kernel/device features throw;
 *  optimized requests never silently fall back to another I/O mode. */
#pragma once
#include "snowy/detail/io.hpp"
#include "snowy/socket.hpp"
#include "snowy/event.hpp"
#include <liburing.h>
#include <span>
#include <new>

// Linux UAPI embeds a zero-length command array in SQEs. GCC diagnoses its
// use in class/coroutine storage even when liburing is a system include.
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#endif

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
    /** @brief Borrow a socket's owner. @param socket Checked socket. */
    static loop& owner(socket& socket);
    /** @brief Borrow a direction reservation. @param socket Socket. @param write Direction. */
    static bool* direction(socket& socket, bool write);
    /** @brief Adopt an accepted descriptor. @param loop Owner. @param fd Transferred fd. */
    static socket adopt(loop& loop, int fd);
    /** @brief Reserve a batch before publishing. @param loop Owner. @param count Slots.
     *  @param drain Wake the internal poll before publishing the chain. */
    static void reserve(loop& loop, unsigned count, bool drain = false);
    /** @brief Publish one preallocated batch entry. @param request Stable state with a retire hook.
     *  @details Caller reserves the entire batch first; no user code runs between entries. */
    static void start(detail::io& request);
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
     *  @param b Optional buffer-table lifetime pin. @param direction Socket reservation.
     *  @details Raw SQEs must be single-shot or SEND_ZC, never multishot; pointed-to
     *  storage stays valid until completion. user_data is reserved by Snowy. */
    op(loop& loop, io_uring_sqe sqe, std::stop_token token = {}, files* f = nullptr,
       buffers* b = nullptr, bool* direction = nullptr);
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

/** @brief Kernel-side ordering within one submission batch. */
enum class link { none, soft, hard };
/** @brief Submit one-shot SQEs together and drain every result, even on cancellation.
 *  @param loop Owner. @param entries Borrowed SQEs and pointed-to resources, valid until return.
 *  @param order No links, soft links (cancel followers on failure), or hard links.
 *  @param token Cancellation. @return Raw CQE results in input order, including negative errno.
 *  @details Supports NOP, positional read/write (ordinary, vector, fixed), FSYNC,
 *  FALLOCATE and SPLICE. user_data and link flags are managed here. No CQE skipping.
 *  DRAIN waits for earlier user I/O, not Snowy's wake poll. Earlier user I/O must
 *  finish naturally, without depending on later cancellation requests. A batch
 *  must fit the SQ. Buffer/file tables must outlive the batch. */
task<std::vector<int>> submit(loop& loop, std::span<const io_uring_sqe> entries,
                             link order = link::none, std::stop_token token = {});

/** @brief Zero-copy send; returns only when the kernel no longer references memory.
 *  @param socket Peer. @param data Borrowed immutable bytes. @param token Cancellation.
 *  @return Partial sent byte count; the kernel may internally copy data. */
[[nodiscard]] op send_zc(socket& socket, std::span<const std::byte> data, std::stop_token token = {});
/** @brief Zero-copy send from registered memory. @param socket Peer. @param table Buffer table.
 *  @param index Buffer slot. @param token Cancellation. @return Partial sent byte count. */
[[nodiscard]] op send_zc(socket& socket, buffers& table, unsigned index, std::stop_token token = {});

/** @brief Owned provided-buffer ring; all access and lease destruction is owner-thread.
 *  @details Keep alive until every stream and chunk is destroyed. Full exhaustion
 *  pauses reception until a chunk is returned; group IDs must be unique per loop. */
class provided {
public:
    /** @brief Allocate and register a buffer ring. @param loop Owner. @param entries Power of two, 1..32768.
     *  @param size Bytes per buffer, 1..INT_MAX. @param group Unique 16-bit group ID. */
    provided(loop& loop, unsigned entries, unsigned size, unsigned short group = 0);
    /** @brief Unregister an idle ring before freeing memory. */
    ~provided();
    provided(const provided&) = delete;
    provided& operator=(const provided&) = delete;
    /** @brief Move-only buffer lease; destruction returns this slot to the kernel. */
    class chunk {
    public:
        /** @brief Construct an empty lease. */
        chunk() = default;
        /** @brief Transfer the lease. @param other Source becoming empty. */
        chunk(chunk&& other) noexcept;
        /** @brief Return this lease and take another. @param other Source. */
        chunk& operator=(chunk&& other) noexcept;
        chunk(const chunk&) = delete;
        chunk& operator=(const chunk&) = delete;
        /** @brief Return the buffer on its owner thread. */
        ~chunk();
        /** @brief Borrow received bytes until lease release. */
        std::span<const std::byte> bytes() const noexcept { return data_; }
    private:
        friend class provided;
        provided* owner_ = nullptr;
        unsigned id_ = 0;
        std::span<const std::byte> data_;
        /** @brief Return a live lease. */
        void release() noexcept;
    };
    /** @brief Decode a kernel-selected buffer. @param flags CQE flags. @param size Nonnegative received size. */
    chunk take(unsigned flags, unsigned size);
    /** @brief Wait for a returned lease if every buffer is held. @param token Cancellation. */
    task<> available(std::stop_token token);
    /** @brief Return the native group ID. */
    unsigned short group() const noexcept { return group_; }
private:
    friend class stream;
    loop& loop_;
    unsigned count_, size_, leases_ = 0, active_ = 0;
    unsigned short group_;
    memory data_, ring_memory_;
    io_uring_buf_ring* ring_;
    std::vector<bool> held_;
    event returned_;
    /** @brief Publish a returned buffer. @param id Previously leased slot. */
    void put(unsigned id) noexcept;
};

/** @brief Internal stable multishot state; consumer exceptions cancel and drain. */
class stream : public detail::io {
public:
    void* context;
    void (*consume)(stream&, int, unsigned); ///< May throw; dispatched behind an exception barrier.
    bool stopped = false;
    std::exception_ptr failure;
    /** @brief Bind one multishot submission. @param socket Borrowed socket.
     *  @param sqe Prepared SQE. @param token Cancellation. @param table Optional provided buffers.
     *  @param context Consumer state. @param consume Callback invoked without coroutine resumption. */
    stream(socket& socket, io_uring_sqe sqe, std::stop_token token, provided* table,
           void* context, void (*consume)(stream&, int, unsigned));
    /** @brief Release the table after terminal and cancel CQEs. */
    ~stream();
    /** @brief Request cancellation without reentering CQ processing. */
    void stop() noexcept;
private:
    io_uring_sqe sqe_;
    provided* table_;
};

/** @brief Receive a TCP byte stream with multishot and owned buffer leases.
 *  @param socket Peer. @param table Provided buffers. @param fn Owned short callback,
 *  bool(chunk); false stops normally. It may transfer chunks to another owner-thread consumer.
 *  @param token Cancellation. @details Rearms on kernel termination/ENOBUFS, never
 *  falls back to single-shot. EOF returns; callback errors rethrow after cleanup. */
template <typename F>
task<> recv(socket& socket, provided& table, F fn, std::stop_token token = {}) {
    struct context { provided& table; F& fn; } context{table, fn};
    for (;;) {
        io_uring_sqe sqe{};
        io_uring_prep_recv_multishot(&sqe, socket.native_handle(), nullptr, 0, 0);
        sqe.flags |= IOSQE_BUFFER_SELECT;
        sqe.buf_group = table.group();
        stream request(socket, sqe, token, &table, &context,
            [](stream& request, int res, unsigned flags) {
                auto& ctx = *static_cast<struct context*>(request.context);
                if (flags & IORING_CQE_F_BUFFER) {
                    auto chunk = ctx.table.take(flags, res > 0 ? static_cast<unsigned>(res) : 0);
                    if (res > 0 && !request.stopped && !std::invoke(ctx.fn, std::move(chunk))) request.stop();
                } else if (res > 0) throw std::runtime_error("multishot missing buffer ID");
            });
        try { co_await request; }
        catch (const std::system_error& e) {
            if (request.failure) std::rethrow_exception(request.failure);
            if (request.stopped && e.code() == std::errc::operation_canceled) co_return;
            if (e.code().value() != ENOBUFS) throw;
        }
        if (request.failure) std::rethrow_exception(request.failure);
        if (request.stopped || (!request.error && !request.bytes)) co_return;
        co_await table.available(token);
    }
}

/** @brief Accept with one multishot request; rearm if the kernel ends a shot.
 *  @param listener Borrowed listening socket. @param fn Owned short bool(socket) callback;
 *  false stops normally. @param token Cancellation. @details Extra accepted sockets
 *  after stop/error are closed; all kernel references are drained before return. */
template <typename F>
task<> accept(socket& listener, F fn, std::stop_token token = {}) {
    for (;;) {
        io_uring_sqe sqe{};
        io_uring_prep_multishot_accept(&sqe, listener.native_handle(), nullptr, nullptr, SOCK_CLOEXEC);
        stream request(listener, sqe, token, nullptr, &fn,
            [](stream& request, int res, unsigned) {
                if (res < 0) return;
                auto peer = access::adopt(request.owner, res);
                if (!request.stopped && !std::invoke(*static_cast<F*>(request.context), std::move(peer))) request.stop();
            });
        try { co_await request; }
        catch (const std::system_error& e) {
            if (request.failure) std::rethrow_exception(request.failure);
            if (request.stopped && e.code() == std::errc::operation_canceled) co_return;
            throw;
        }
        if (request.failure) std::rethrow_exception(request.failure);
        if (request.stopped) co_return;
    }
}
} // namespace snowy::uring
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic pop
#endif
