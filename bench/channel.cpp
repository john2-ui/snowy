/** @file channel.cpp
 *  @brief Single-loop bounded FIFO throughput; includes coroutine call overhead. */
#include "common.hpp"
#include <snowy/snowy.hpp>

/** @brief Produce ordered integers. @param c Channel. @param count Messages. */
snowy::task<> send(snowy::channel<unsigned>& c, unsigned count) {
    for (unsigned i = 0; i < count; ++i) co_await c.send(i);
    c.close();
}
/** @brief Verify count and order. @param c Channel. @param count Expected messages. */
snowy::task<> recv(snowy::channel<unsigned>& c, unsigned count) {
    unsigned seen = 0;
    while (auto value = co_await c.recv()) {
        if (*value != seen++) throw std::runtime_error("channel order mismatch");
    }
    if (seen != count) throw std::runtime_error("channel count mismatch");
}
/** @brief Warm up, then report nine sample means.
 *  @param argc Argument count. @param argv Message count and capacity. */
int main(int argc, char** argv) {
    try {
        if (argc > 3) throw std::invalid_argument("usage: snowy_channel [messages] [capacity]");
        const auto count = argc > 1 ? bench::count(argv[1]) : 1'000'000u;
        const auto capacity = argc > 2 ? bench::count(argv[2]) : 1024u;
        std::vector<double> samples;
        for (unsigned i = 0; i < 10; ++i) {
            snowy::loop loop;
            snowy::channel<unsigned> c(loop, capacity);
            loop.spawn(send(c, count)); loop.spawn(recv(c, count));
            const auto start = bench::clock::now();
            loop.run();
            if (i) samples.push_back(bench::elapsed(start) / count);
        }
        bench::report("channel message", samples);
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
