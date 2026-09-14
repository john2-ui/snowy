/** @file when.hpp
 *  @brief Structured all/any joins and cooperative timeouts; never abandon children. */
#pragma once
#include "snowy/event.hpp"
#include <array>
#include <tuple>

namespace snowy::detail {
template <typename T>
using result = std::conditional_t<std::is_void_v<T>, std::monostate, T>;
/** @brief Noncancelable lifetime barrier owned by the parent. */
struct join {
    loop& owner;
    event done;
    std::exception_ptr error;
    std::stop_source stop;
    std::size_t pending = 0;
    std::optional<std::size_t> winner;
    /** @brief Bind the cleanup barrier. @param loop Owner. */
    explicit join(loop& loop) : owner(loop), done(loop) {}
};
/** @brief Dynamically sized homogeneous result slots. */
template <typename T>
struct group : join {
    std::vector<std::optional<result<T>>> values;
    /** @brief Allocate stable result slots. @param loop Owner. @param size Children. */
    group(loop& loop, std::size_t size) : join(loop), values(size) {}
};
/** @brief Catch every child error and publish only after frame cleanup.
 *  @param input Child task. @param state Parent state. @param value Result slot.
 *  @param index Input position.
 *  @param race Whether first completion requests sibling cancellation. */
template <typename T>
detached collect(task<T> input, join& state, std::optional<result<T>>& value, std::size_t index, bool race) {
    co_await state.owner.schedule();
    std::exception_ptr error;
    try {
        if constexpr (std::is_void_v<T>) { co_await std::move(input); value.emplace(); }
        else value.emplace(co_await std::move(input));
    } catch (...) { error = std::current_exception(); }
    if (race) {
        if (!state.winner) {
            state.winner = index;
            state.error = error;
            state.stop.request_stop();
        }
    } else if (error && !state.error) state.error = error;
    if (!--state.pending) state.done.set();
}
}

namespace snowy {
/** @brief Start all homogeneous tasks; return results in input order after draining.
 *  @param loop Owner. @param tasks Consumed tasks; empty input succeeds.
 *  @return Values, with monostate for void children.
 *  @throws First observed error, only after all started tasks complete.
 *  @details No implicit fail-fast cancellation: pass shared tokens to children
 *  when needed. Every child must eventually complete or honor loop.stop(). */
template <typename T>
task<std::vector<detail::result<T>>> when_all(loop& loop, std::vector<task<T>> tasks) {
    loop.check();
    detail::group<T> state(loop, tasks.size());
    for (std::size_t i = 0; i < tasks.size(); ++i) {
        ++state.pending;
        try { detail::collect(std::move(tasks[i]), state, state.values[i], i, false); }
        catch (...) { --state.pending; state.error = std::current_exception(); break; }
    }
    if (state.pending) co_await state.done.join();
    if (state.error) std::rethrow_exception(state.error);
    std::vector<detail::result<T>> values;
    values.reserve(tasks.size());
    for (auto& value : state.values) values.push_back(std::move(*value));
    co_return values;
}

/** @brief Join heterogeneous tasks using frame-local result slots, not a heap vector.
 *  @param loop Owner. @param tasks Consumed tasks; an empty pack succeeds.
 *  @return Tuple in input order, with monostate for void tasks.
 *  @throws First observed error after all started children finish. */
template <typename... T>
task<std::tuple<detail::result<T>...>> when_all(loop& loop, task<T>... tasks) {
    loop.check();
    detail::join state(loop);
    std::tuple<std::optional<detail::result<T>>...> values;
    auto launch = [&](auto& input, auto& value) {
        if (state.error) return;
        ++state.pending;
        try { detail::collect(std::move(input), state, value, 0, false); }
        catch (...) { --state.pending; state.error = std::current_exception(); }
    };
    std::apply([&](auto&... value) { (launch(tasks, value), ...); }, values);
    if (state.pending) co_await state.done.join();
    if (state.error) std::rethrow_exception(state.error);
    co_return std::apply([](auto&... value) {
        return std::tuple<detail::result<T>...>{std::move(*value)...};
    }, values);
}

/** @brief Return the first completion, cancel siblings, then drain all of them.
 *  @param loop Owner. @param factories Owned factories receiving a shared stop token.
 *  @return Winning input index and result (monostate for void).
 *  @throws Winner's error after draining, or invalid_argument for empty input.
 *  @details Factories must return promptly; children must honor cancellation or
 *  finish naturally. A synchronous factory/startup failure cancels started children. */
template <typename T>
task<std::pair<std::size_t, detail::result<T>>> when_any(
    loop& loop, std::vector<std::function<task<T>(std::stop_token)>> factories) {
    loop.check();
    if (factories.empty()) throw std::invalid_argument("empty when_any");
    detail::group<T> state(loop, factories.size());
    std::exception_ptr startup;
    for (std::size_t i = 0; i < factories.size(); ++i) {
        ++state.pending;
        try { detail::collect(factories[i](state.stop.get_token()), state, state.values[i], i, true); }
        catch (...) {
            --state.pending;
            startup = std::current_exception();
            state.stop.request_stop();
            break;
        }
    }
    if (state.pending) co_await state.done.join();
    if (startup) std::rethrow_exception(startup);
    if (state.error) std::rethrow_exception(state.error);
    co_return std::pair<std::size_t, detail::result<T>>{
        *state.winner, std::move(*state.values[*state.winner])};
}

namespace detail {
/** @brief Await a typed factory without type erasure. @param fn Parent-owned factory.
 *  @param token Shared cancellation token. */
template <typename T, typename F>
task<T> invoke(F& fn, std::stop_token token) { co_return co_await std::invoke(fn, token); }
}

/** @brief Race typed, possibly move-only factories without std::function.
 *  @param loop Owner. @param factories Owned token-aware factories with result T.
 *  @return Winning index and value, after every child has drained. */
template <typename T, typename... F>
    requires (sizeof...(F) > 0 && (std::invocable<F&, std::stop_token> && ...))
task<std::pair<std::size_t, detail::result<T>>> when_any(loop& loop, F... factories) {
    loop.check();
    detail::join state(loop);
    std::array<std::optional<detail::result<T>>, sizeof...(F)> values;
    std::exception_ptr startup;
    std::size_t index = 0;
    auto launch = [&](auto& factory) {
        if (startup) return;
        ++state.pending;
        try {
            detail::collect(detail::invoke<T>(factory, state.stop.get_token()), state, values[index], index, true);
            ++index;
        }
        catch (...) { --state.pending; startup = std::current_exception(); state.stop.request_stop(); }
    };
    (launch(factories), ...);
    if (state.pending) co_await state.done.join();
    if (startup) std::rethrow_exception(startup);
    if (state.error) std::rethrow_exception(state.error);
    co_return std::pair<std::size_t, detail::result<T>>{*state.winner, std::move(*values[*state.winner])};
}

/** @brief Apply a cooperative deadline without type-erasing the callable.
 *  @param loop Owner. @param delay Timeout duration. @param factory Token-aware task factory.
 *  @throws timed_out if the timer wins; cleanup may extend beyond the deadline. */
template <typename T, typename F>
task<T> timeout(loop& loop, loop::clock::duration delay,
                F factory) {
    auto result = co_await when_any<T>(loop, std::move(factory), [&loop, delay](std::stop_token token) -> task<T> {
        co_await loop.sleep(delay, token);
        throw std::system_error(std::make_error_code(std::errc::timed_out));
    });
    if constexpr (!std::is_void_v<T>) co_return std::move(result.second);
}
} // namespace snowy
