/** @file uring.hpp
 *  @brief Explicit Linux optimizations; borrowed resources live through completion.
 *  @details Requires liburing 2.3+. Unsupported kernel/device features throw;
 *  optimized requests never silently fall back to another I/O mode. */
#pragma once
#include "snowy/detail/io.hpp"
#include "snowy/socket.hpp"
#include "snowy/udp.hpp"
#include "snowy/event.hpp"
#include <liburing.h>
#include <span>
#include <new>
#include <array>
#include <algorithm>

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
    unsigned submit_batch = 0; ///< Flush threshold; zero waits for a full SQ or native poll.
    int wq_fd = -1; ///< Borrow another ring's ordinary fd during setup to share io-wq.
    bool register_fd = false; ///< Register the ring descriptor on its owner thread (5.18+).
    bool single_issuer = true; ///< Opportunistic SINGLE_ISSUER; disable on cross-thread clone sources.
};

/** @brief Internal bridge; applications use the typed operations below. */
class access {
public:
    /** @brief Borrow an owner-thread ring. @param loop Owner. */
    static io_uring& ring(loop& loop);
    /** @brief Borrow the immutable ordinary ring fd from any thread. @param loop Live owner.
     *  @details The caller prevents concurrent loop destruction. */
    static int fd(const loop& loop) noexcept;
    /** @brief Borrow a socket's owner. @param socket Checked socket. */
    static loop& owner(socket& socket);
    /** @brief Borrow a direction reservation. @param socket Socket. @param write Direction. */
    static bool* direction(socket& socket, bool write);
    /** @brief Adopt an accepted descriptor. @param loop Owner. @param fd Transferred fd. */
    static socket adopt(loop& loop, int fd);
    /** @brief Borrow the datagram socket's native ownership state. @param socket UDP owner. */
    static snowy::socket& socket_of(udp& socket);
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
    /** @brief Allocate empty registered slots. @param loop Owner. @param count Positive slots. */
    files(loop& loop, unsigned count);
    /** @brief Unregister an idle table; destroying a table referenced by ops terminates. */
    ~files();
    files(const files&) = delete;
    files& operator=(const files&) = delete;
    /** @brief Replace slots in an idle table; -1 removes a descriptor.
     *  @param offset First slot. @param fds Replacements. @return Number updated.
     *  @details Kernel updates may be partial. All referencing op objects must be destroyed. */
    unsigned update(unsigned offset, std::span<const int> fds);
    /** @brief Open directly into a registered slot, returning zero on success.
     *  @param index Destination slot. @param path Borrowed null-terminated path.
     *  @param flags O_* flags. @param mode Creation permissions. @param token Cancellation.
     *  @details Path survives the await. Serialize all uses of the destination slot. */
    [[nodiscard]] op open(unsigned index, const char* path, int flags, unsigned mode = 0, std::stop_token token = {});
    /** @brief Close a direct descriptor. @param index Slot, serialized against other uses.
     *  @param token Cancellation. */
    [[nodiscard]] op close(unsigned index, std::stop_token token = {});
    /** @brief Create a socket directly in a slot. @param index Destination slot.
     *  @param domain AF_*. @param type SOCK_*. @param protocol IPPROTO_*. @param token Cancellation. */
    [[nodiscard]] op socket(unsigned index, int domain, int type, int protocol = 0, std::stop_token token = {});
    /** @brief Accept directly into a slot, returning zero. @param index Destination slot.
     *  @param listener Ordinary listening socket on this loop. @param token Cancellation. */
    [[nodiscard]] op accept(unsigned index, snowy::socket& listener, std::stop_token token = {});
    /** @brief Receive using a fixed socket slot. @param index Slot. @param data Destination.
     *  @param flags MSG_*. @param token Cancellation. @details Serialize reads on each slot. */
    [[nodiscard]] op recv(unsigned index, std::span<std::byte> data, int flags = 0, std::stop_token token = {});
    /** @brief Send using a fixed socket slot. @param index Slot. @param data Source.
     *  @param flags MSG_*. @param token Cancellation. @details Serialize writes on each slot. */
    [[nodiscard]] op send(unsigned index, std::span<const std::byte> data, int flags = 0, std::stop_token token = {});
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
    /** @brief Allocate empty registered buffer slots. @param loop Owner. @param count Positive slots. */
    buffers(loop& loop, unsigned count);
    /** @brief Clone another ring's registrations without repinning (liburing 2.9+, Linux 6.12+).
     *  @param loop Destination. @param source Stable source table, externally synchronized.
     *  @details Source updates/destruction must not race construction. Borrowed memory
     *  outlives both independent tables; later source updates do not alter this clone.
     *  Cross-thread sources must opt out of SINGLE_ISSUER; newer kernels enforce its affinity. */
    buffers(loop& loop, const buffers& source);
    /** @brief Unregister before releasing memory, after all operations are destroyed. */
    ~buffers();
    buffers(const buffers&) = delete;
    buffers& operator=(const buffers&) = delete;
    /** @brief Replace idle slots; null/zero regions remove registrations.
     *  @param offset First slot. @param regions Replacements. @return Number updated.
     *  @details Kernel updates may be partial. Destroy referencing ops before updating. */
    unsigned update(unsigned offset, std::span<const iovec> regions);
    /** @brief Borrow a registered region. @param index Buffer slot. */
    std::span<std::byte> at(unsigned index) const;
private:
    friend class op;
    friend class files;
    loop& loop_;
    std::vector<iovec> regions_;
    std::size_t active_ = 0;
};

/** @brief Set io-wq limits and return previous values. @param loop Owner.
 *  @param limits Bounded and unbounded worker limits; zero queries without changing. */
std::array<unsigned, 2> workers(loop& loop, std::array<unsigned, 2> limits);
/** @brief Set/reset io-wq CPU affinity. @param loop Owner. @param mask CPU set, or nullptr to reset. */
void affinity(loop& loop, const cpu_set_t* mask);
/** @brief Configure NAPI busy polling (liburing 2.6+, suitable kernel/NIC required).
 *  @param loop Owner. @param usecs Busy-poll duration. @param prefer Prefer busy polling.
 *  @details Zero duration disables busy polling. Configuration success is not proof of NIC support. */
void napi(loop& loop, unsigned usecs, bool prefer = false);

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
/** @brief Split zero-copy send completion from buffer release for pipelining.
 *  @details Await sent() once, then join() before destruction or buffer reuse.
 *  A failed sent() already drains kernel references before throwing. The socket,
 *  table and loop outlive this object. Buffer ownership remains with the caller. */
class zc_send : public op {
    event sent_, released_;
    bool started_ = false;
    /** @brief Bind shared send/release handling. @param socket Peer. @param sqe Prepared send.
     *  @param token Cancellation. @param table Optional pinned buffer table. */
    zc_send(socket& socket, io_uring_sqe sqe, std::stop_token token, buffers* table);
    /** @brief Submit exactly once, including immediate cancellation completion. */
    void start();
public:
    /** @brief Borrow immutable bytes. @param socket Peer. @param data Source. @param token Cancellation. */
    zc_send(socket& socket, std::span<const std::byte> data, std::stop_token token = {});
    /** @brief Borrow registered bytes. @param socket Peer. @param table Buffer table.
     *  @param index Slot. @param token Cancellation. */
    zc_send(socket& socket, buffers& table, unsigned index, std::stop_token token = {});
    /** @brief First-CQE waiter, embedded in the consumer frame. */
    struct awaiter {
        zc_send& source;
        event::awaiter ready;
        /** @brief Bind the noncancelable primary barrier. @param source Send state. */
        explicit awaiter(zc_send& source) : source(source), ready(source.sent_.join()) {}
        /** @brief Enter the one-shot submission hook. */
        bool await_ready() const noexcept { return false; }
        /** @brief Submit and await primary completion. @param h Continuation. */
        bool await_suspend(std::coroutine_handle<> h) { source.start(); return ready.await_suspend(h); }
        /** @brief Return the partial byte count, or throw after failed-send cleanup. */
        std::size_t await_resume() { return source.detail::io::await_resume(); }
    };
    /** @brief Start and await sent bytes; success does not permit buffer reuse yet. */
    [[nodiscard]] awaiter sent() { return awaiter{*this}; }
    /** @brief Wait until the buffer is reusable, including shutdown/cancellation cleanup. */
    [[nodiscard]] event::awaiter join() {
        if (!started_) throw std::logic_error("zero-copy send not started");
        return released_.join();
    }
    bool await_suspend(std::coroutine_handle<>) = delete; ///< Use sent()/join(), never await this object directly.
};

/** @brief Send gathered bytes with zero-copy notification draining. @param socket Peer.
 *  @param message Borrowed header, vectors and payload. @param token Cancellation. */
[[nodiscard]] op sendmsg_zc(socket& socket, const msghdr& message, std::stop_token token = {});

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
    /** @brief Decode a bundled CQE in publication order, even after out-of-order returns.
     *  @param flags CQE flags. @param size Total received bytes. @param fn Consumer of each chunk.
     *  @details On callback failure, return the unconsumed leases before rethrowing. */
    template <typename F>
    void each(unsigned flags, unsigned size, F fn) {
        const unsigned parts = size ? (size - 1) / size_ + 1 : 1;
        if (parts > count_) throw std::runtime_error("invalid receive bundle size");
        unsigned id = flags >> IORING_CQE_BUFFER_SHIFT;
        std::exception_ptr failure;
        for (unsigned i = 0; i < parts; ++i) {
            if (id >= count_) throw std::runtime_error("invalid receive bundle ID");
            const unsigned next = next_[id]; // Returning this lease may change its next publication.
            const unsigned bytes = std::min(size, size_);
            auto value = take((flags & 0xffffu) | (id << IORING_CQE_BUFFER_SHIFT), bytes);
            size -= bytes;
            if (!failure) {
                try { std::invoke(fn, std::move(value)); }
                catch (...) { failure = std::current_exception(); }
            }
            id = next;
        }
        if (failure) std::rethrow_exception(failure);
    }
    /** @brief Wait for a returned lease if every buffer is held. @param token Cancellation. */
    task<> available(std::stop_token token);
    /** @brief Return the native group ID. */
    unsigned short group() const noexcept { return group_; }
    /** @brief Return the byte capacity of one provided buffer. */
    unsigned size() const noexcept { return size_; }
private:
    friend class stream;
    loop& loop_;
    unsigned count_, size_, leases_ = 0, active_ = 0;
    unsigned short group_;
    memory data_, ring_memory_;
    io_uring_buf_ring* ring_;
    std::vector<bool> held_;
    std::vector<unsigned short> next_; ///< Publication successor, not numeric ID successor.
    unsigned tail_; ///< Last published buffer ID.
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

/** @brief Received datagram owning its provided-buffer lease, including metadata.
 *  @details bytes/name/control remain valid until this message is destroyed.
 *  MSG_TRUNC/MSG_CTRUNC expose truncation; wire_size is the original payload length. */
struct message {
    provided::chunk storage;
    std::span<const std::byte> bytes, name, control;
    unsigned flags = 0, wire_size = 0;
    /** @brief Validate and decode one recvmsg completion. @param data Owned lease.
     *  @param layout Input metadata capacities, not the output lengths. */
    message(provided::chunk data, msghdr layout);
};

/** @brief Receive datagrams with multishot recvmsg and owned metadata/payload.
 *  @param socket UDP peer. @param table Provided buffers. @param fn Owned bool(message) consumer.
 *  @param token Cancellation. @param control Ancillary byte capacity per message.
 *  @details False stops normally. Empty datagrams are delivered; callback failures drain first. */
template <typename F>
task<> recvmsg(udp& socket, provided& table, F fn, std::stop_token token = {}, unsigned control = 0) {
    auto& native = access::socket_of(socket);
    msghdr layout{};
    layout.msg_namelen = sizeof(sockaddr_storage);
    layout.msg_controllen = control;
    if (std::uint64_t{control} + sizeof(sockaddr_storage) + sizeof(io_uring_recvmsg_out) > table.size())
        throw std::invalid_argument("recvmsg metadata exceeds provided buffer");
    struct context { provided& table; F& fn; msghdr& layout; } context{table, fn, layout};
    for (;;) {
        io_uring_sqe sqe{};
        io_uring_prep_recvmsg_multishot(&sqe, native.native_handle(), &layout, MSG_TRUNC);
        sqe.flags |= IOSQE_BUFFER_SELECT;
        sqe.buf_group = table.group();
        stream request(native, sqe, token, &table, &context,
            [](stream& request, int res, unsigned flags) {
                auto& ctx = *static_cast<struct context*>(request.context);
                if (flags & IORING_CQE_F_BUFFER) {
                    auto chunk = ctx.table.take(flags, res > 0 ? static_cast<unsigned>(res) : 0);
                    if (res >= 0 && !request.stopped && !std::invoke(ctx.fn, message{std::move(chunk), ctx.layout}))
                        request.stop();
                } else if (res >= 0) throw std::runtime_error("recvmsg missing buffer ID");
            });
        try { co_await request; }
        catch (const std::system_error& e) {
            if (request.failure) std::rethrow_exception(request.failure);
            if (request.stopped && e.code() == std::errc::operation_canceled) co_return;
            if (e.code().value() != ENOBUFS) throw;
        }
        if (request.failure) std::rethrow_exception(request.failure);
        if (request.stopped) co_return;
        co_await table.available(token);
    }
}

/** @brief Receive a TCP byte stream with multishot and owned buffer leases.
 *  @param socket Peer. @param table Provided buffers. @param fn Owned short callback,
 *  bool(chunk); false stops normally. It may transfer chunks to another owner-thread consumer.
 *  @param token Cancellation. @param bundle Fill multiple buffers per CQE (Linux 6.10+).
 *  @details Rearms on kernel termination/ENOBUFS, never
 *  falls back to single-shot. EOF returns; callback errors rethrow after cleanup. */
template <typename F>
task<> recv(socket& socket, provided& table, F fn, std::stop_token token = {}, bool bundle = false) {
#ifndef IORING_RECVSEND_BUNDLE
    if (bundle) throw std::system_error(std::make_error_code(std::errc::operation_not_supported), "receive bundles need newer liburing");
#endif
    struct context { provided& table; F& fn; bool bundle; } context{table, fn, bundle};
    for (;;) {
        io_uring_sqe sqe{};
        io_uring_prep_recv_multishot(&sqe, socket.native_handle(), nullptr, 0, 0);
#ifdef IORING_RECVSEND_BUNDLE
        if (bundle) sqe.ioprio |= IORING_RECVSEND_BUNDLE;
#endif
        sqe.flags |= IOSQE_BUFFER_SELECT;
        sqe.buf_group = table.group();
        stream request(socket, sqe, token, &table, &context,
            [](stream& request, int res, unsigned flags) {
                auto& ctx = *static_cast<struct context*>(request.context);
                if (flags & IORING_CQE_F_BUFFER) {
                    auto consume = [&](provided::chunk chunk) {
                        if (res > 0 && !request.stopped && !std::invoke(ctx.fn, std::move(chunk))) request.stop();
                    };
                    const auto size = res > 0 ? static_cast<unsigned>(res) : 0;
                    if (ctx.bundle) ctx.table.each(flags, size, consume);
                    else consume(ctx.table.take(flags, size));
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
