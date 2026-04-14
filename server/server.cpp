/**
 * MoldUDP64 ITCH Sender
 *
 * Reads an ITCH binary file via mmap and sends messages over UDP
 * wrapped in MoldUDP64 packets. Zero-copy from file to packet buffer.
 *
 * Usage:
 *   ./sender <itch_file> burst              Send as fast as possible
 *   ./sender <itch_file> throttled [rate]   Send at rate msg/sec (default: 100000)
 *   ./sender <itch_file> realtime [speed]   Replay at original timestamps (default: 1000x)
 */

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <chrono>
#include <thread>

#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

// =============================================
// Constants
// =============================================

static constexpr const char* HOST        = "127.0.0.1";
static constexpr int         PORT        = 12345;
static constexpr int         MAX_BATCH   = 20;
static constexpr int         MAX_PKT     = 1400;
static constexpr size_t      HEADER_SIZE = 20;
static constexpr uint16_t    END_OF_SESSION = 0xFFFF;
static constexpr char        SESSION[10] = {'T','E','S','T','0','0','0','0','0','1'};

// =============================================
// Endian helpers
// =============================================

static inline uint16_t read_be16(const uint8_t* p) {
    return (uint16_t(p[0]) << 8) | uint16_t(p[1]);
}

static inline uint64_t read_be48(const uint8_t* p) {
    uint64_t val = 0;
    for (int i = 0; i < 6; i++) val = (val << 8) | p[i];
    return val;
}

static inline void write_be16(uint8_t* p, uint16_t val) {
    p[0] = (val >> 8) & 0xFF;
    p[1] = val & 0xFF;
}

static inline void write_be64(uint8_t* p, uint64_t val) {
    for (int i = 7; i >= 0; i--) { p[i] = val & 0xFF; val >>= 8; }
}

// =============================================
// Timing
// =============================================

using Clock     = std::chrono::steady_clock;
using TimePoint = Clock::time_point;
using Duration  = std::chrono::nanoseconds;

// Hybrid sleep
static inline void sleep_until_approx(TimePoint target) {
    auto remaining = target - Clock::now();
    auto threshold = std::chrono::microseconds(100);

    // Hard sleep
    if (remaining > threshold) std::this_thread::sleep_for(remaining - threshold);
    
    // Busy wait (regular sleep is not reliable because of syscall, context switch etc)
    while (Clock::now() < target) {}
}

// =============================================
// MMap file
// =============================================

struct MappedFile {
    // Make uint8_t for easier pointer arithmetics
    const uint8_t* data = nullptr;
    size_t size = 0;
    int fd = -1;

    bool open(const char* path) {
        fd = ::open(path, O_RDONLY);
        if (fd < 0) { 
            perror("open"); 
            return false;
        }

        struct stat st;
        if (fstat(fd, &st) < 0) { 
            perror("fstat"); close(fd); return false; 
        }
        size = st.st_size;

        // mmap file (ITCH file) onto virtual memory
        void* ptr = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (ptr == MAP_FAILED) { 
            perror("mmap"); 
            close(fd); 
            return false; 
        }

        // Performance boost (advise that we will only be doing sequential reads)
        madvise(ptr, size, MADV_SEQUENTIAL);

        data = static_cast<const uint8_t*>(ptr);
        return true;
    }

    ~MappedFile() {
        if (data) munmap(const_cast<uint8_t*>(data), size);
        if (fd >= 0) close(fd);
    }
};

// =============================================
// ITCH cursor (zero-copy over mmap'd data)
// =============================================

struct ITCHCursor {
    const uint8_t* base;
    size_t file_size;
    size_t offset = 0;

    ITCHCursor(const uint8_t* data, size_t size) : base(data), file_size(size) {}

    bool has_next() const { return offset + 2 <= file_size; }

    const uint8_t* next(uint16_t& msg_len) {
        // Length
        if (offset + 2 > file_size) return nullptr;
        msg_len = read_be16(base + offset);
        offset += 2;

        // Body
        if (offset + msg_len > file_size) return nullptr;
        const uint8_t* body = base + offset;
        offset += msg_len;
        return body;
    }

    static uint64_t timestamp(const uint8_t* body, uint16_t len) {
        if (len < 11) return 0;
        return read_be48(body + 5);
    }
};

// =============================================
// UDP socket
// =============================================

struct UDPSocket {
    int fd = -1;
    struct sockaddr_in dest{};

    bool open() {
        fd = socket(AF_INET, SOCK_DGRAM, 0);
        if (fd < 0) { perror("socket"); return false; }

        // Default socket buffer might overflow OS so just be prepared
        int buf = 4 * 1024 * 1024;
        setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &buf, sizeof(buf));

        dest.sin_family = AF_INET;
        dest.sin_port = htons(PORT);
        inet_pton(AF_INET, HOST, &dest.sin_addr);
        return true;
    }

    void send(const uint8_t* data, size_t len) {
        sendto(fd, data, len, 0,
               reinterpret_cast<const sockaddr*>(&dest), sizeof(dest));
    }

    ~UDPSocket() { 
        if (fd >= 0) close(fd); 
    }
};

// =============================================
// Packet builder (reusable, zero-alloc)
// =============================================

struct PacketBuilder {
    // theoretical 65535 (max UDP size) - 20 (IP header) - 8 (UDP header)
    // capped at 1400 but anyways just good to know
    uint8_t buf[65507];
    size_t offset = HEADER_SIZE;
    int count = 0;

    // Write session ID and sequence number
    void reset(uint64_t seq) {
        memcpy(buf, SESSION, 10);
        write_be64(buf + 10, seq);
        offset = HEADER_SIZE;
        count = 0;
    }

    // Add one ITCH message
    bool try_append(const uint8_t* body, uint16_t len) {
        if (offset + 2 + len > MAX_PKT) return false;
        write_be16(buf + offset, len);
        memcpy(buf + offset + 2, body, len);
        offset += 2 + len;
        count++;
        return true;
    }

    // Writes the final message count into bytes 18-19 of the header
    size_t finalize() {
        write_be16(buf + 18, static_cast<uint16_t>(count));
        return offset;
    }

    // Special header-only packet (0xFF length) indicating session finished
    size_t end_of_session(uint64_t seq) {
        memcpy(buf, SESSION, 10);
        write_be64(buf + 10, seq);
        write_be16(buf + 18, END_OF_SESSION);
        return HEADER_SIZE;
    }
};

// =============================================
// Send helpers
// =============================================

struct Stats {
    uint64_t msgs = 0;
    uint64_t pkts = 0;
    uint64_t seq  = 1;
};

static void flush(PacketBuilder& pkt, UDPSocket& sock, Stats& s) {
    if (pkt.count == 0) return;
    size_t sz = pkt.finalize();
    
    // Actually send
    sock.send(pkt.buf, sz);
    
    // Adding metadata
    s.msgs += pkt.count;
    s.pkts++;
    s.seq += pkt.count;
}

// =============================================
// Send modes
// =============================================

// Use full hardware
Stats send_burst(UDPSocket& sock, ITCHCursor& cursor) {
    PacketBuilder pkt;
    Stats s;
    pkt.reset(s.seq);

    while (cursor.has_next()) {
        uint16_t len;
        const uint8_t* body = cursor.next(len);
        if (!body) break;

        if (pkt.count >= MAX_BATCH || !pkt.try_append(body, len)) {
            flush(pkt, sock, s);
            pkt.reset(s.seq);
            pkt.try_append(body, len);
        }
    }

    // Last socket
    flush(pkt, sock, s);
    return s;
}

// Send data stream into a perfectly uniform rhythm
Stats send_throttled(UDPSocket& sock, ITCHCursor& cursor, int rate) {
    PacketBuilder pkt;
    Stats s;

    // e.g. if target rate is 100k messages/sec and MAX_BATCH is 20,
    // send one packet ever 200k nanoseconds
    double interval_ns = (double(MAX_BATCH) / rate) * 1e9;
    auto delay = Duration(int64_t(interval_ns));
    auto next_send = Clock::now();

    pkt.reset(s.seq);

    while (cursor.has_next()) {
        uint16_t len;
        const uint8_t* body = cursor.next(len);
        if (!body) break;

        if (pkt.count >= MAX_BATCH || !pkt.try_append(body, len)) {
            sleep_until_approx(next_send);
            flush(pkt, sock, s);
            next_send += delay;
            pkt.reset(s.seq);
            pkt.try_append(body, len);
        }
    }

    sleep_until_approx(next_send);
    flush(pkt, sock, s);
    return s;
}

Stats send_realtime(UDPSocket& sock, ITCHCursor& cursor, double speed) {
    PacketBuilder pkt;
    Stats s;

    uint16_t first_len;
    const uint8_t* first_body = cursor.next(first_len);
    if (!first_body) return s;

    // Get the base timestamp of first packet
    uint64_t base_ts = ITCHCursor::timestamp(first_body, first_len);
    uint64_t prev_ts = base_ts;
    auto wall_start = Clock::now();

    pkt.reset(s.seq);
    pkt.try_append(first_body, first_len);

    while (cursor.has_next()) {
        uint16_t len;
        const uint8_t* body = cursor.next(len);
        if (!body) break;

        uint64_t ts = ITCHCursor::timestamp(body, len);

        // If the timestamp hasn't changed by at least 1ms compared to the previous message, 
        // consider it simultaneous
        bool ts_changed = (ts > prev_ts) && (ts - prev_ts > 1000);

        // Send if
        // 1. Belong to new microsecond
        // 2. MAX_BATCH exceeded
        if ((ts_changed || pkt.count >= MAX_BATCH) && pkt.count > 0) {
            double elapsed_ns = double(ts - base_ts) / speed;
            auto target = wall_start + Duration(int64_t(elapsed_ns));
            sleep_until_approx(target);

            flush(pkt, sock, s);
            pkt.reset(s.seq);
        }

        pkt.try_append(body, len);
        prev_ts = ts;
    }
    flush(pkt, sock, s);
    return s;
}

// =============================================
// Main
// =============================================

int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr,
            "Usage: %s <itch_file> <mode> [param]\n"
            "\n"
            "Modes:\n"
            "  burst                Send as fast as possible\n"
            "  throttled [rate]     Send at rate msg/sec (default: 100000)\n"
            "  realtime  [speed]    Replay at speed multiplier (default: 1000)\n",
            argv[0]);
        return 1;
    }

    const char* file_path = argv[1];
    const char* mode = argv[2];

    MappedFile file;
    if (!file.open(file_path)) return 1;

    // Count messages before starting
    size_t msg_count = 0;
    {
        ITCHCursor scan(file.data, file.size);
        while (scan.has_next()) {
            uint16_t len;
            if (!scan.next(len)) break;
            msg_count++;
        }
    }
    printf("Loaded %s: %zu messages, %zu bytes\n", file_path, msg_count, file.size);

    UDPSocket sock;
    if (!sock.open()) return 1;

    printf("Sending to %s:%d [%s]\n\n", HOST, PORT, mode);

    ITCHCursor cursor(file.data, file.size);
    Stats s;

    auto start = Clock::now();

    if (strcmp(mode, "burst") == 0) {
        s = send_burst(sock, cursor);
    } else if (strcmp(mode, "throttled") == 0) {
        int rate = (argc > 3) ? atoi(argv[3]) : 100000;
        printf("  Rate: %d msg/sec\n", rate);
        s = send_throttled(sock, cursor, rate);
    } else if (strcmp(mode, "realtime") == 0) {
        double speed = (argc > 3) ? atof(argv[3]) : 1000.0;
        printf("  Speed: %.0fx\n", speed);
        s = send_realtime(sock, cursor, speed);
    } else {
        fprintf(stderr, "Unknown mode: %s\n", mode);
        return 1;
    }

    double elapsed = std::chrono::duration<double>(Clock::now() - start).count();

    // End-of-session (3x)
    PacketBuilder pkt;
    for (int i = 0; i < 3; i++) {
        size_t sz = pkt.end_of_session(s.seq);
        sock.send(pkt.buf, sz);
        usleep(100000);
    }

    printf("\nDone: %lu messages, %lu packets, %.3fs",
           s.msgs, s.pkts, elapsed);
    if (elapsed > 0)
        printf(" (%.0f msg/sec)", s.msgs / elapsed);
    printf("\n");

    return 0;
}
