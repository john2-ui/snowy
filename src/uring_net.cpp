/** @file uring_net.cpp
 *  @brief Multishot leases and zero-copy sends with terminal-CQE lifetime barriers. */
#include "snowy/uring.hpp"
#include <bit>
#include <climits>
#include <limits>

namespace snowy::uring {
loop& access::owner(socket& socket) { socket.check(); return *socket.loop_; }
bool* access::direction(socket& socket, bool write) {
    socket.check();
    return write ? &socket.writing_ : &socket.reading_;
}
socket access::adopt(loop& loop, int fd) { return socket{loop, fd}; }

op send_zc(socket& socket, std::span<const std::byte> data, std::stop_token token) {
    if (data.size() > INT_MAX) throw std::invalid_argument("send_zc size overflow");
    io_uring_sqe sqe{};
    io_uring_prep_send_zc(&sqe, socket.native_handle(), data.data(), data.size(), MSG_NOSIGNAL, 0);
    return {access::owner(socket), sqe, token, nullptr, nullptr, access::direction(socket, true)};
}
op send_zc(socket& socket, buffers& table, unsigned index, std::stop_token token) {
    auto data = table.at(index);
    io_uring_sqe sqe{};
    io_uring_prep_send_zc_fixed(&sqe, socket.native_handle(), data.data(), data.size(), MSG_NOSIGNAL, 0, index);
    return {access::owner(socket), sqe, token, nullptr, &table, access::direction(socket, true)};
}

namespace {
/** @brief Validate dimensions before allocation. @param count Ring entries. @param size Slot bytes. */
std::size_t capacity(unsigned count, unsigned size) {
    if (!count || count > 32768 || !std::has_single_bit(count) || !size || size > INT_MAX ||
        size > std::numeric_limits<std::size_t>::max() / count)
        throw std::invalid_argument("invalid provided buffers");
    return std::size_t{count} * size;
}
}
provided::provided(loop& loop, unsigned entries, unsigned size, unsigned short group)
    : loop_(loop), count_(entries), size_(size), group_(group), data_(capacity(entries, size)),
      ring_memory_(std::size_t{entries} * sizeof(io_uring_buf)),
      ring_(reinterpret_cast<io_uring_buf_ring*>(ring_memory_.bytes().data())), held_(entries), returned_(loop) {
    io_uring_buf_reg reg{};
    reg.ring_addr = reinterpret_cast<std::uintptr_t>(ring_);
    reg.ring_entries = entries;
    reg.bgid = group;
    io_uring_buf_ring_init(ring_);
    const int result = io_uring_register_buf_ring(&access::ring(loop), &reg, 0);
    if (result < 0) throw std::system_error(-result, std::generic_category(), "provided buffer ring");
    for (unsigned i = 0; i < entries; ++i)
        io_uring_buf_ring_add(ring_, data_.bytes().data() + std::size_t{i} * size_, size_,
                             static_cast<unsigned short>(i), static_cast<int>(count_ - 1), static_cast<int>(i));
    io_uring_buf_ring_advance(ring_, static_cast<int>(entries));
}
provided::~provided() {
    if (active_ || leases_ || io_uring_unregister_buf_ring(&access::ring(loop_), group_) < 0) std::terminate();
}
provided::chunk provided::take(unsigned flags, unsigned size) {
    loop_.check();
    const unsigned id = flags >> IORING_CQE_BUFFER_SHIFT;
    if (!(flags & IORING_CQE_F_BUFFER) || id >= count_ || held_[id] || size > size_)
        throw std::runtime_error("invalid provided buffer completion");
    held_[id] = true;
    ++leases_;
    chunk result;
    result.owner_ = this;
    result.id_ = id;
    result.data_ = data_.bytes().subspan(std::size_t{id} * size_, size);
    return result;
}
void provided::put(unsigned id) noexcept {
    loop_.check();
    held_[id] = false;
    --leases_;
    io_uring_buf_ring_add(ring_, data_.bytes().data() + std::size_t{id} * size_, size_,
                         static_cast<unsigned short>(id), static_cast<int>(count_ - 1), 0);
    io_uring_buf_ring_advance(ring_, 1);
    returned_.set();
}
task<> provided::available(std::stop_token token) {
    while (leases_ == count_) {
        returned_.reset();
        co_await returned_.wait(token);
    }
}
provided::chunk::chunk(chunk&& other) noexcept
    : owner_(std::exchange(other.owner_, nullptr)), id_(other.id_), data_(other.data_) {}
provided::chunk& provided::chunk::operator=(chunk&& other) noexcept {
    if (this != &other) {
        release();
        owner_ = std::exchange(other.owner_, nullptr);
        id_ = other.id_;
        data_ = other.data_;
    }
    return *this;
}
void provided::chunk::release() noexcept { if (owner_) std::exchange(owner_, nullptr)->put(id_); }
provided::chunk::~chunk() { release(); }

stream::stream(socket& socket, io_uring_sqe sqe, std::stop_token token, provided* table,
               void* context, void (*consume)(stream&, int, unsigned))
    : detail::io(access::owner(socket), detail::opcode::native, -1, nullptr, 0, token,
                 access::direction(socket, false)), context(context), consume(consume), sqe_(sqe), table_(table) {
    if (table && &table->loop_ != &owner) throw std::invalid_argument("provided buffers belong to another loop");
    if (table) ++table->active_;
    prepare = [](detail::io& base, io_uring_sqe& entry) noexcept { entry = static_cast<stream&>(base).sqe_; };
    result = [](detail::io& base, int res, unsigned flags) noexcept {
        auto& self = static_cast<stream&>(base);
        if (res < 0) self.error = {-res, std::generic_category()};
        else self.bytes = static_cast<std::size_t>(res);
        try { self.consume(self, res, flags); }
        catch (...) {
            if (!self.failure) self.failure = std::current_exception();
            self.stop();
        }
    };
}
stream::~stream() {
    if (active) std::terminate();
    if (table_) --table_->active_;
}
void stream::stop() noexcept {
    stopped = true;
    detail::op::cancel_fn{this}();
}
} // namespace snowy::uring
