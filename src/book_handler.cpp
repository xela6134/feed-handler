#include "spsc_queue.h"
#include "mold_parser.h"
#include "itch_parser.h"
#include "order_book.h"

void book_handler_main(SPSCQueue* queue) {
    uint64_t expected_seq = 1;
    // Order books, allocators, etc. initialised here

    while (true) {
        const Packet* pkt = queue->try_peek();
        if (!pkt) continue;  // spin until data arrives

        // Parse MoldUDP64 header
        // Check sequence number for gaps
        // Iterate message blocks
        // For each ITCH message: parse type, update order book

        queue->pop();
    }
}
