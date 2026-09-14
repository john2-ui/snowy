/** @file when.hpp
 *  @brief Structured all/any joins and cooperative timeouts; never abandon children. */
#pragma once
#include "snowy/event.hpp"
#include <array>
#include <tuple>

namespace snowy::detail {
template <typename T>
using result = std::conditional_t<std::is_void_v<T>, std::monostate, T>;
/** @brief Inspect the awaiter protocol without adding a coroutine. @param value Awaitable. */
template <typename T>
decltype(auto) get_awaiter(T&& value) {
    if constexpr (requires { std::forward<T>(value).operator co_await(); })
        return std::forward<T>(value).operator co_await();
    else if constexpr (requires { operator co_await(std::forward<T>(value)); })
        return operator co_await(std::forward<T>(value));
    else return std::forward<T>(value);
}
template <typename T>
using await_result = decltype(get_awaiter(std::declval<T>()).await_resume());
template <typename A>
concept direct_awaiter = requires(A& a, std::coroutine_handle<> h) {
    a.await_ready(); a.await_suspend(h); a.await_resume();
};
/** @brief Copyable reference adapter for immovable lvalue awaiters. */
template <typename A>
struct await_ref {
    A& value;
    /** @brief Forward the readiness check. */
    bool await_ready() { return value.await_ready(); }
    /** @brief Forward every supported suspension return type. @param h Continuation. */
    decltype(auto) await_suspend(std::coroutine_handle<> h) { return value.await_suspend(h); }
    /** @brief Preserve the awaited result type. */
    decltype(auto) await_resume() { return value.await_resume(); }
};
template <typename F>
using race_result = result<std::remove_cvref_t<await_result<std::invoke_result_t<F&, std::stop_token>>>>;
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
 *  @param input Owned factory returning a task or direct awaiter. @param state Parent state. @param value Result slot.
 *  @param index Input position.
 *  @param race Whether first completion requests sibling cancellation. */
template <typename F, typename R>
detached collect(F input, join& state, std::optional<R>& value, std::size_t index, bool race) {
    co_await state.owner.schedule();
    std::exception_ptr error;
    std::optional<R> local;
    try {
        if constexpr (std::is_void_v<await_result<decltype(input())>>) { co_await input(); local.emplace(); }
        else local.emplace(co_await input());
    } catch (...) { error = std::current_exception(); }
    co_await state.owner.on();
    if (!error) {
        try { value.emplace(std::move(*local)); }
        catch (...) { error = std::current_exception(); }
    }
    if (race) {
        if (!state.winner) {
            state.winner = index;
            state.error = error;
            state.stop.request_stop();
        }
    } else if (error && !state.error) state.error = error;
    if (!--state.pending) state.done.set();
}

/** @brief Own factories and fixed-count result slots in one parent frame.
 *  @param loop Owner. @param factories Owned zero-argument awaitable factories. */
template <typename... F>
task<std::tuple<result<std::remove_cvref_t<await_result<std::invoke_result_t<F&>>>>...>>
all(loop& loop, F... factories) {
    loop.check();
    join state(loop);
    std::tuple<std::optional<result<std::remove_cvref_t<await_result<std::invoke_result_t<F&>>>>>...> values;
    auto launch = [&](auto& factory, auto& value) {
        if (state.error) return;
        ++state.pending;
        try { collect([&factory]() -> decltype(auto) { return std::invoke(factory); }, state, value, 0, false); }
        catch (...) { --state.pending; state.error = std::current_exception(); }
    };
    std::apply([&](auto&... value) { (launch(factories, value), ...); }, values);
    if (state.pending) co_await state.done.join();
    if (state.error) std::rethrow_exception(state.error);
    co_return std::apply([](auto&... value) {
        return std::tuple<result<std::remove_cvref_t<await_result<std::invoke_result_t<F&>>>>...>{std::move(*value)...};
    }, values);
}
}

namespace snowy {
/** @brief Start all homogeneous tasks; return results in input order after draining.
 *  @param loop Owner. @param tasks Consumed tasks; empty input succeeds.
 *  @return Values, with monostate for void children.
 *  @throws First observed error, only after all started tasks complete.
 *  @details No implicit fail-fast cancellation: pass shared tokens to children
 *  when needed. Every child must eventually complete or honor loop.stop(). */
template <typename T, typename A>
task<std::vector<detail::result<T>>> when_all(loop& loop, std::vector<task<T, A>> tasks) {
    loop.check();
    detail::group<T> state(loop, tasks.size());
    for (std::size_t i = 0; i < tasks.size(); ++i) {
        ++state.pending;
        try { detail::collect([input = std::move(tasks[i])]() mutable { return std::move(input); },
                              state, state.values[i], i, false); }
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
template <typename... T, typename... A>
task<std::tuple<detail::result<T>...>> when_all(loop& loop, task<T, A>... tasks) {
    return detail::all(loop, [child = std::move(tasks)]() mutable { return std::move(child); }...);
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
        try {
            detail::collect([input = factories[i](state.stop.get_token())]() mutable { return std::move(input); },
                            state, state.values[i], i, true);
        }
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
            detail::collect([&factory, token = state.stop.get_token()] { return std::invoke(factory, token); },
                            state, values[index], index, true);
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

/** @brief Join borrowed direct awaiters without wrapping each in a task.
 *  @param loop Owner. @param inputs Stable lvalue awaiters, each awaited once.
 *  @details Inputs outlive the returned task and its completion. One completion
 *  bridge per input is still required by the coroutine continuation protocol. */
template <typename... A>
    requires (sizeof...(A) > 0 && (detail::direct_awaiter<A> && ...))
task<std::tuple<detail::result<std::remove_cvref_t<detail::await_result<A&>>>...>>
when_all(loop& loop, A&... inputs) {
    return detail::all(loop, [&inputs] { return detail::await_ref<A>{inputs}; }...);
}

/** @brief Join owned zero-argument factories, including nonmovable I/O awaiters.
 *  @param loop Owner. @param factories Owned factories; referenced buffers must outlive the join. */
template <typename... F>
    requires (sizeof...(F) > 0 && (std::invocable<F&> && ...))
auto when_all(loop& loop, F... factories) { return detail::all(loop, std::move(factories)...); }

/** @brief Race heterogeneous token-aware factories returning tasks or direct awaiters.
 *  @param loop Owner. @param factories Owned factories; cancellation must eventually finish.
 *  @return Variant indexed by input position; void results use monostate.
 *  @throws Winner's error, after every started operation has drained. */
template <typename... F>
    requires (sizeof...(F) > 0 && (std::invocable<F&, std::stop_token> && ...))
task<std::variant<detail::race_result<F>...>> when_any(loop& loop, F... factories) {
    loop.check();
    detail::join state(loop);
    std::tuple<std::optional<detail::race_result<F>>...> values;
    std::exception_ptr startup;
    std::size_t index = 0;
    auto launch = [&](auto& factory, auto& value) {
        if (startup) return;
        ++state.pending;
        try {
            detail::collect([&factory, token = state.stop.get_token()] { return std::invoke(factory, token); },
                            state, value, index++, true);
        } catch (...) { --state.pending; startup = std::current_exception(); state.stop.request_stop(); }
    };
    std::apply([&](auto&... value) { (launch(factories, value), ...); }, values);
    if (state.pending) co_await state.done.join();
    if (startup) std::rethrow_exception(startup);
    if (state.error) std::rethrow_exception(state.error);
    std::optional<std::variant<detail::race_result<F>...>> output;
    [&]<std::size_t... I>(std::index_sequence<I...>) {
        ((*state.winner == I ? (void)output.emplace(std::in_place_index<I>, std::move(*std::get<I>(values))) : (void)0), ...);
    }(std::index_sequence_for<F...>{});
    co_return std::move(*output);
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
