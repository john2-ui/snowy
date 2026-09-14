/** @file pmr.hpp
 *  @brief Opt-in polymorphic allocator for task frames. */
#pragma once
#include "snowy/task.hpp"
#include <memory_resource>

namespace snowy::pmr {
/** @brief Resource-backed task; pass a polymorphic_allocator<byte> first.
 *  @details A thread-unsafe resource requires allocation and destruction to be
 *  externally serialized. No allocator propagates implicitly to child tasks. */
template <typename T = void>
using task = snowy::task<T, std::pmr::polymorphic_allocator<std::byte>>;
}
