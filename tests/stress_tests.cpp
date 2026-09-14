/** @file stress_tests.cpp
 *  @brief Race cross-thread cancellation against batched user and native completions. */
#include <snowy/snowy.hpp>
#include <charconv>
#include <cstdlib>
#include <iostream>

/** @brief Complete once, whether notification or cancellation wins.
 *  @param loop Owner. @param event Shared signal. @param token Stop token.
 *  @param count Completion count. @param native Use an io_uring/IOCP NOP when true. */
snowy::task<> wait(snowy::loop& loop, snowy::event& event, std::stop_token token,
                   unsigned& count, bool native) {
    try {
        if (native) co_await snowy::detail::io{loop, snowy::detail::opcode::nop,
            snowy::detail::invalid_socket, nullptr, 0, token};
        else co_await event.wait(token);
    } catch (const std::system_error& e) {
        if (e.code() != std::errc::operation_canceled) throw;
    }
    ++count;
}
/** @brief Publish after other roots have started. @param started Cross-thread flag. */
snowy::task<> signal(std::atomic_bool& started) {
    started.store(true, std::memory_order_release);
    co_return;
}
/** @brief Run repeated races with fresh operation addresses.
 *  @param argc Argument count. @param argv Optional positive round count. */
int main(int argc, char** argv) {
    try {
        unsigned rounds = 100;
        if (argc > 2) throw std::invalid_argument("usage: snowy_stress_tests [rounds]");
        if (argc == 2) {
            const std::string_view text = argv[1];
            const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), rounds);
            if (error != std::errc{} || end != text.data() + text.size() || !rounds || rounds > 100000)
                throw std::invalid_argument("invalid round count");
        }
        for (unsigned round = 0; round < rounds; ++round) {
            snowy::loop loop;
            snowy::event event(loop);
            std::stop_source stop;
            std::atomic_bool started{false};
            unsigned count = 0;
            for (unsigned i = 0; i < 256; ++i)
                loop.spawn(wait(loop, event, stop.get_token(), count, i % 2 == 0));
            loop.spawn(signal(started));
            std::thread producer([&] {
                while (!started.load(std::memory_order_acquire)) std::this_thread::yield();
                loop.post([&] { event.set(); });
                stop.request_stop();
            });
            loop.run();
            producer.join();
            loop.run();
            if (count != 256) std::abort();
        }
        std::cout << rounds << " rounds passed\n";
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
