/** @file iocp.cpp
 *  @brief Overlapped socket I/O and batched Windows completion dispatch. */
#include "snowy/detail/io.hpp"
#include <algorithm>
#include <limits>

namespace snowy {
struct loop::driver { HANDLE port = nullptr; };

loop::loop() : driver_(std::make_unique<driver>()) {
    WSADATA data;
    int r = WSAStartup(MAKEWORD(2, 2), &data);
    if (r) throw std::system_error(r, std::system_category(), "WSAStartup");
    driver_->port = CreateIoCompletionPort(INVALID_HANDLE_VALUE, nullptr, 0, 1);
    if (!driver_->port) {
        const auto error = GetLastError();
        WSACleanup();
        throw std::system_error(static_cast<int>(error), std::system_category(), "IOCP");
    }
}

loop::~loop() {
    if (running_ || roots_ || io_ || waits_ || !timers_.empty() || !ready_.empty()) std::terminate();
    CloseHandle(driver_->port);
    WSACleanup();
}

void loop::wake() noexcept {
    if (!PostQueuedCompletionStatus(driver_->port, 0, 0, nullptr)) std::terminate();
}

void loop::attach(std::uintptr_t fd) {
    if (!CreateIoCompletionPort(reinterpret_cast<HANDLE>(fd), driver_->port, 0, 0))
        throw std::system_error(static_cast<int>(GetLastError()), std::system_category(), "associate socket");
}

namespace {
/** @brief Normalize portable socket completion errors. @param code Winsock status. */
std::error_code io_error(int code) {
    if (code == WSA_OPERATION_ABORTED) return std::make_error_code(std::errc::operation_canceled);
    if (code == WSAEMSGSIZE) return std::make_error_code(std::errc::message_size);
    return {code, std::system_category()};
}
/** @brief Query a provider-specific Winsock extension.
 *  @param fd Socket provider. @param id Extension identifier. */
template <typename T>
T extension(SOCKET fd, GUID id) {
    T function = nullptr;
    DWORD bytes = 0;
    if (WSAIoctl(fd, SIO_GET_EXTENSION_FUNCTION_POINTER, &id, sizeof(id),
                 &function, sizeof(function), &bytes, nullptr, nullptr) != 0)
        throw std::system_error(WSAGetLastError(), std::system_category(), "WSAIoctl");
    return function;
}
}

void loop::submit(detail::io& op) {
    op.overlapped.request = &op;
    if (op.code == detail::opcode::nop) {
        if (!PostQueuedCompletionStatus(driver_->port, 0, 0, &op.overlapped))
            throw std::system_error(static_cast<int>(GetLastError()), std::system_category());
        return;
    }
    op.buffer.buf = static_cast<char*>(op.data);
    op.buffer.len = op.size;
    DWORD bytes = 0, flags = 0;
    int result = 0;
    switch (op.code) {
    case detail::opcode::nop: break;
    case detail::opcode::read:
        result = WSARecv(op.fd, &op.buffer, 1, &bytes, &flags, &op.overlapped, nullptr); break;
    case detail::opcode::write:
        result = WSASend(op.fd, &op.buffer, 1, &bytes, 0, &op.overlapped, nullptr); break;
    case detail::opcode::recv_from:
        result = WSARecvFrom(op.fd, &op.buffer, 1, nullptr, &op.flags,
            reinterpret_cast<sockaddr*>(&op.address), &op.address_size, &op.overlapped, nullptr); break;
    case detail::opcode::send_to:
        result = WSASendTo(op.fd, &op.buffer, 1, nullptr, 0,
            reinterpret_cast<sockaddr*>(&op.address), op.address_size, &op.overlapped, nullptr); break;
    case detail::opcode::accept: {
        sockaddr_storage address{};
        int size = sizeof(address);
        if (getsockname(op.fd, reinterpret_cast<sockaddr*>(&address), &size))
            throw std::system_error(WSAGetLastError(), std::system_category(), "getsockname");
        const auto accept = extension<LPFN_ACCEPTEX>(op.fd, WSAID_ACCEPTEX);
        op.accepted = WSASocketW(address.ss_family, SOCK_STREAM, IPPROTO_TCP, nullptr, 0,
                                WSA_FLAG_OVERLAPPED | WSA_FLAG_NO_HANDLE_INHERIT);
        if (op.accepted == INVALID_SOCKET)
            throw std::system_error(WSAGetLastError(), std::system_category(), "WSASocket");
        result = accept(op.fd, op.accepted, op.accept_buffer, 0,
                        sizeof(sockaddr_storage) + 16, sizeof(sockaddr_storage) + 16,
                        &bytes, &op.overlapped) ? 0 : SOCKET_ERROR;
        break;
    }
    case detail::opcode::connect: {
        const auto connect = extension<LPFN_CONNECTEX>(op.fd, WSAID_CONNECTEX);
        sockaddr_storage local{};
        local.ss_family = op.address.ss_family;
        if (::bind(op.fd, reinterpret_cast<sockaddr*>(&local), op.address_size) != 0)
            throw std::system_error(WSAGetLastError(), std::system_category(), "ConnectEx bind");
        result = connect(op.fd, reinterpret_cast<sockaddr*>(&op.address), op.address_size,
                         nullptr, 0, &bytes, &op.overlapped) ? 0 : SOCKET_ERROR;
        break;
    }
    }
    if (result == SOCKET_ERROR) {
        const int error = WSAGetLastError();
        if (error != WSA_IO_PENDING) {
            op.error = io_error(error);
            complete(op);
        }
    }
    // Synchronous success also queues an IOCP packet; never resume it twice.
}

void loop::cancel(detail::io& op) {
    op.cancel_sent = true;
    if (op.code == detail::opcode::nop) return;
    if (!CancelIoEx(reinterpret_cast<HANDLE>(op.fd), &op.overlapped)) {
        const auto error = GetLastError();
        if (error != ERROR_NOT_FOUND)
            throw std::system_error(static_cast<int>(error), std::system_category(), "CancelIoEx");
    }
}

void loop::poll(std::chrono::nanoseconds delay) {
    DWORD timeout = INFINITE;
    if (delay.count() >= 0) {
        const auto ms = std::chrono::ceil<std::chrono::milliseconds>(delay).count();
        timeout = static_cast<DWORD>(std::min<std::int64_t>(ms, INFINITE - 1));
    }
    OVERLAPPED_ENTRY entries[128];
    ULONG count = 0;
    if (!GetQueuedCompletionStatusEx(driver_->port, entries, 128, &count, timeout, FALSE)) {
        const auto error = GetLastError();
        if (error == WAIT_TIMEOUT) return;
        throw std::system_error(static_cast<int>(error), std::system_category(), "IOCP wait");
    }
    for (ULONG i = 0; i < count; ++i) {
        if (!entries[i].lpOverlapped) continue;
        auto& op = *static_cast<detail::io::packet*>(entries[i].lpOverlapped)->request;
        op.bytes = entries[i].dwNumberOfBytesTransferred;
        if (op.code != detail::opcode::nop) {
            DWORD bytes = 0, flags = 0;
            // Only the error path needs NTSTATUS -> Winsock error conversion.
            if (entries[i].Internal != 0 &&
                !WSAGetOverlappedResult(op.fd, &op.overlapped, &bytes, FALSE, &flags)) {
                const int error = WSAGetLastError();
                op.error = io_error(error);
            }
            if (!op.error && op.code == detail::opcode::accept &&
                setsockopt(op.accepted, SOL_SOCKET, SO_UPDATE_ACCEPT_CONTEXT,
                           reinterpret_cast<const char*>(&op.fd), sizeof(op.fd)))
                op.error = {WSAGetLastError(), std::system_category()};
            if (!op.error && op.code == detail::opcode::connect &&
                setsockopt(op.fd, SOL_SOCKET, SO_UPDATE_CONNECT_CONTEXT, nullptr, 0))
                op.error = {WSAGetLastError(), std::system_category()};
        }
        complete(op);
    }
}
} // namespace snowy
