/** @file mailbox.cpp
 *  @brief Pass bounded messages between two independent loop threads. */
#include <snowy/snowy.hpp>
#include <iostream>

/** @brief Send a finite stream. @param loop Sender loop. @param box Shared mailbox. */
snowy::task<> send(snowy::loop& loop, snowy::mailbox<int>& box) {
    for (int i = 0; i < 10; ++i) co_await box.send(loop, i);
    box.close();
}
/** @brief Drain through close. @param loop Receiver loop. @param box Shared mailbox. */
snowy::task<> recv(snowy::loop& loop, snowy::mailbox<int>& box) {
    while (auto value = co_await box.recv(loop)) std::cout << *value << '\n';
}
/** @brief Join the producer before destroying the shared queue. */
int main() {
    snowy::mailbox<int> box(2);
    std::jthread producer([&] { snowy::loop loop; loop.run(send(loop, box)); });
    snowy::loop loop;
    loop.run(recv(loop, box));
}
