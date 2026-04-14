#include "spsc_queue.h"
#include <thread>
#include <cstdio>
#include <sys/mman.h>

void pin_to_core(int core) {
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(core, &set);
    pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
}

// Defined in receiver.cpp and book_handler.cpp
void receiver_main(SPSCQueue* queue);
void book_handler_main(SPSCQueue* queue);

int main() {
    // Allocate queue on huge pages, pre-fault, lock
    auto* queue = static_cast<SPSCQueue*>(
        mmap(nullptr, sizeof(SPSCQueue),
             PROT_READ | PROT_WRITE,
             MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE,
             -1, 0));
    new (queue) SPSCQueue{};
    mlock(queue, sizeof(SPSCQueue));

    std::thread receiver([queue]() {
        pin_to_core(2);   // isolated P-core
        receiver_main(queue);
    });

    std::thread book_handler([queue]() {
        pin_to_core(4);   // different isolated P-core
        book_handler_main(queue);
    });

    receiver.join();
    book_handler.join();

    munmap(queue, sizeof(SPSCQueue));
    return 0;
}
