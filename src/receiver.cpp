#include "spsc_queue.h"
#include <sys/socket.h>
#include <arpa/inet.h>
#include <cstdio>

void receiver_main(SPSCQueue* queue) {
    int fd = socket(AF_INET, SOCK_DGRAM, 0);

    int buf_size = 16 * 1024 * 1024;
    setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &buf_size, sizeof(buf_size));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(12345);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    bind(fd, (sockaddr*)&addr, sizeof(addr));

    uint8_t recv_buf[65536];

    while (true) {
        ssize_t n = recvfrom(fd, recv_buf, sizeof(recv_buf), 0, nullptr, nullptr);
        if (n <= 0) continue;

        // Push entire UDP payload (MoldUDP64 packet) into the queue
        if (!queue->try_push(recv_buf, static_cast<uint16_t>(n))) {
            // Queue full, book handler can't keep up
            // Log this, it means you're dropping data
        }
    }
}
