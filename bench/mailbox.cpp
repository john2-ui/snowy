/** @file mailbox.cpp
 *  @brief Two-loop FIFO throughput; includes wakeups, order validation and thread teardown. */
#include "common.hpp"
#include <snowy/snowy.hpp>
#include <future>
#include <array>

/** @brief Send a bounded ordered stream. @param loop Owner. @param box Shared channel.
 *  @param count Messages. */
snowy::task<> send(snowy::loop& loop, snowy::mailbox<unsigned>& box, unsigned count) {
    for (unsigned i = 0; i < count; ++i) co_await box.send(loop, i);
    box.close();
}
/** @brief Verify exact FIFO delivery through close. @param loop Owner. @param box Channel.
 *  @param count Expected messages. */
snowy::task<> recv(snowy::loop& loop, snowy::mailbox<unsigned>& box, unsigned count) {
    unsigned seen = 0;
    while (auto value = co_await box.recv(loop))
        if (*value != seen++) throw std::runtime_error("mailbox order mismatch");
    if (seen != count) throw std::runtime_error("mailbox count mismatch");
}
/** @brief Exclude queue/loop/thread construction using a shared start signal.
 *  @param count Messages. @param capacity Queue slots. @return Mean ns per delivery. */
double measure(unsigned count, unsigned capacity) {
    snowy::mailbox<unsigned> box(capacity);
    std::array<std::promise<void>, 2> ready;
    std::promise<void> begin;
    auto go = begin.get_future().share();
    std::array<std::exception_ptr, 2> errors{};
    auto worker = [&](unsigned index) {
        bool running = false;
        try {
            snowy::loop loop;
            loop.spawn(index ? recv(loop, box, count) : send(loop, box, count));
            running = true;
            ready[index].set_value();
            go.wait();
            loop.run();
        } catch (...) {
            errors[index] = std::current_exception();
            box.close();
            if (!running) ready[index].set_value();
        }
    };
    std::jthread producer(worker, 0u);
    std::jthread consumer;
    try { consumer = std::jthread(worker, 1u); }
    catch (...) { box.close(); begin.set_value(); throw; }
    for (auto& signal : ready) signal.get_future().wait();
    const auto start = bench::clock::now();
    begin.set_value();
    producer.join(); consumer.join();
    const auto elapsed = bench::elapsed(start);
    for (auto error : errors) if (error) std::rethrow_exception(error);
    return elapsed / count;
}
/** @brief Report nine means after warmup. @param argc Count. @param argv Messages and capacity. */
int main(int argc, char** argv) {
    try {
        if (argc > 3) throw std::invalid_argument("usage: snowy_mailbox [messages] [capacity]");
        const auto count = argc > 1 ? bench::count(argv[1]) : 100000u;
        const auto capacity = argc > 2 ? (std::string_view(argv[2]) == "0" ? 0u : bench::count(argv[2])) : 1024u;
        std::vector<double> samples;
        for (unsigned i = 0; i < 10; ++i) {
            const auto ns = measure(count, capacity);
            if (i) samples.push_back(ns);
        }
        bench::report("mailbox message", samples);
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
