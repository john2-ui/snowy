/**
 * @file intrusive_queue.hpp
 * @brief Provides the allocation-free FIFO used by an event-loop thread.
 */

#pragma once

#include <cassert>
#include <cstddef>

namespace snowy::detail {

/**
 * @brief Link embedded in work that can be placed on a ready queue.
 * @details A node may belong to at most one queue. The queue never owns it.
 */
class ready_operation {
public:
    ready_operation() = default;
    ready_operation(const ready_operation&) = delete;
    ready_operation& operator=(const ready_operation&) = delete;

private:
    friend class ready_queue;
    ready_operation* next_ = nullptr;
};

/**
 * @brief A single-threaded intrusive FIFO with constant-time insertion.
 */
class ready_queue {
public:
    ready_queue() = default;
    ready_queue(const ready_queue&) = delete;
    ready_queue& operator=(const ready_queue&) = delete;

    /**
     * @brief Reports whether the queue contains no operations.
     * @return True when the queue is empty.
     */
    [[nodiscard]] bool empty() const noexcept { return head_ == nullptr; }

    /**
     * @brief Adds an operation to the tail.
     * @param operation Operation whose lifetime exceeds its queue membership.
     */
    void push(ready_operation& operation) noexcept {
        assert(operation.next_ == nullptr);
        auto* node = &operation;
        if (tail_ == nullptr) {
            head_ = node;
        } else {
            tail_->next_ = node;
        }
        tail_ = node;
    }

    /**
     * @brief Removes the operation at the head.
     * @return The next operation, or nullptr when the queue is empty.
     */
    ready_operation* pop() noexcept {
        auto* node = head_;
        if (node == nullptr) {
            return nullptr;
        }

        head_ = node->next_;
        node->next_ = nullptr;
        if (head_ == nullptr) {
            tail_ = nullptr;
        }
        return node;
    }

private:
    ready_operation* head_ = nullptr;
    ready_operation* tail_ = nullptr;
};

} // namespace snowy::detail
