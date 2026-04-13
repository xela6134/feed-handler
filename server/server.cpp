/**
 * MoldUDP64 ITCH Sender
 *
 * Reads an ITCH binary file via mmap and sends messages over UDP
 * wrapped in MoldUDP64 packets. Zero-copy from file to packet buffer.
 *
 * MoldUDP64 packet format (20-byte header + message blocks):
 * ┌──────────────────────────────────────────────────────────┐
 * │ Session (10 bytes, ASCII, right-padded spaces)           │
 * │ Sequence Number (8 bytes, big-endian uint64)             │
 * │ Message Count (2 bytes, big-endian uint16)               │
 * ├──────────────────────────────────────────────────────────┤
 * │ Message Block 1: [2-byte BE length][message data]        │
 * │ Message Block 2: [2-byte BE length][message data]        │
 * │ ...                                                      │
 * └──────────────────────────────────────────────────────────┘
 *
 * Usage:
 *   ./moldudp_sender <itch_file> [options]
 *
 * Options:
 *   --host <ip>       Destination IP (default: 127.0.0.1)
 *   --port <port>     Destination port (default: 12345)
 *   --mode <mode>     burst | throttled | realtime (default: throttled)
 *   --rate <n>        Messages/sec for throttled mode (default: 100000)
 *   --batch <n>       Max messages per packet (default: 20)
 *   --max-pkt <n>     Max UDP payload bytes (default: 1400)
 *   --session <str>   Session ID, max 10 chars (default: TEST000001)
 *   --speed <x>       Time multiplier for realtime mode (default: 1.0)
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
// Endian helpers
// =============================================

static inline uint16_t read_be16(const uint8_t* p) {
    return (uint16_t(p[0]) << 8) | uint16_t(p[1]);
}

static inline uint64_t read_be48(const uint8_t* p) {
    // 6-byte big-endian timestamp
    uint64_t val = 0;
    for (int i = 0; i < 6; i++) {
        val = (val << 8) | p[i];
    }
    return val;
}

static inline void write_be16(uint8_t* p, uint16_t val) {
    p[0] = (val >> 8) & 0xFF;
    p[1] = val & 0xFF;
}

static inline void write_be64(uint8_t* p, uint64_t val) {
    for (int i = 7; i >= 0; i--) {
        p[i] = val & 0xFF;
        val >>= 8;
    }
}

// =============================================
// MoldUDP64 constants
// =============================================

static constexpr size_t MOLD_HEADER_SIZE = 20;
static constexpr uint16_t END_OF_SESSION = 0xFFFF;
static constexpr size_t MAX_UDP_PAYLOAD = 65507;

// =============================================
// Configuration
// =============================================

enum class SendMode { BURST, THROTTLED, REALTIME };

struct Config {
    const char* itch_file = nullptr;
    const char* host      = "127.0.0.1";
    int         port      = 12345;
    SendMode    mode      = SendMode::THROTTLED;
    int         rate      = 100000;
    int         batch     = 20;
    int         max_pkt   = 1400;
    char        session[10];
    double      speed     = 1.0;

    Config() {
        memcpy(session, "TEST000001", 10);
    }
};

// =============================================
// MMap file wrapper
// =============================================

struct MappedFile {
    // data[0] is 1st byte, data[100] is 101st byte, etc
    // Made into uint8_t for easy pointer arithmetics
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
            perror("fstat");
            close(fd);
            return false;
        }
        size = st.st_size;

        // mmap the entire file read-only
        void* ptr = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (ptr == MAP_FAILED) {
            perror("mmap");
            close(fd);
            return false;
        }

        // Hint: sequential access pattern
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
// ITCH message cursor over mmap'd data
// =============================================

struct ITCHCursor {
    const uint8_t* base;
    size_t file_size;
    size_t offset;

    ITCHCursor(const uint8_t* data, size_t size)
        : base(data), file_size(size), offset(0) {}

    bool has_next() const {
        return offset + 2 <= file_size;
    }

    // Returns pointer to message body and sets msg_len.
    // Does NOT copy - points directly into mmap'd memory.
    const uint8_t* next(uint16_t& msg_len) {
        if (offset + 2 > file_size) return nullptr;

        msg_len = read_be16(base + offset);
        offset += 2;

        if (offset + msg_len > file_size) return nullptr;

        const uint8_t* body = base + offset;
        offset += msg_len;
        return body;
    }

    // Extract 6-byte timestamp from an ITCH message body
    // Timestamp is at offset 5 (after msg_type[1] + stock_locate[2] + tracking[2])
    static uint64_t extract_timestamp(const uint8_t* body, uint16_t len) {
        if (len < 11) return 0;
        return read_be48(body + 5);
    }

    void reset() { offset = 0; }
};

// ─────────────────────────────────────────────
// UDP socket wrapper
// ─────────────────────────────────────────────

struct UDPSocket {
    int fd = -1;
    struct sockaddr_in dest{};

    bool open(const char* host, int port) {
        // IPv4, UDP
        fd = socket(AF_INET, SOCK_DGRAM, 0);
        if (fd < 0) {
            perror("socket");
            return false;
        }

        // Increase send buffer
        // OS has a small buffer, but might be slower than NIC
        // Expand that buffer to 4MB
        int buf_size = 4 * 1024 * 1024;
        setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &buf_size, sizeof(buf_size));

        dest.sin_family = AF_INET;

        //     htons: making port little endian byte format
        // inet_pton: making IP address little endian byte format
        dest.sin_port = htons(port);
        if (inet_pton(AF_INET, host, &dest.sin_addr) != 1) {
            fprintf(stderr, "Invalid address: %s\n", host);
            return false;
        }

        return true;
    }

    ssize_t send(const uint8_t* data, size_t len) {
        return sendto(fd, data, len, 0,
                      reinterpret_cast<const sockaddr*>(&dest), sizeof(dest));
    }

    ~UDPSocket() {
        if (fd >= 0) close(fd);
    }
};

// ─────────────────────────────────────────────
// Packet builder (reusable buffer, zero-alloc in hot path)
// ─────────────────────────────────────────────

struct PacketBuilder {
    uint8_t buf[MAX_UDP_PAYLOAD];
    size_t offset;
    int msg_count;
    const char* session;
    size_t max_pkt;

    PacketBuilder(const char* sess, size_t max_pkt_size)
        : offset(MOLD_HEADER_SIZE), msg_count(0), session(sess),
          max_pkt(max_pkt_size) {}

    void reset(uint64_t seq_num) {
        // Write header (finalize msg_count later)
        memcpy(buf, session, 10);
        write_be64(buf + 10, seq_num);
        // msg_count written in finalize()
        offset = MOLD_HEADER_SIZE;
        msg_count = 0;
    }

    // Try to append a message. Returns false if it doesn't fit.
    bool try_append(const uint8_t* msg_body, uint16_t msg_len) {
        size_t block_size = 2 + msg_len;
        if (offset + block_size > max_pkt) return false;

        write_be16(buf + offset, msg_len);
        memcpy(buf + offset + 2, msg_body, msg_len);
        offset += block_size;
        msg_count++;
        return true;
    }

    // Finalize: write message count into header
    size_t finalize() {
        write_be16(buf + 18, static_cast<uint16_t>(msg_count));
        return offset;
    }

    // Build a heartbeat packet
    size_t build_heartbeat(uint64_t next_seq) {
        memcpy(buf, session, 10);
        write_be64(buf + 10, next_seq);
        write_be16(buf + 18, 0);
        return MOLD_HEADER_SIZE;
    }

    // Build an end-of-session packet
    size_t build_end_of_session(uint64_t next_seq) {
        memcpy(buf, session, 10);
        write_be64(buf + 10, next_seq);
        write_be16(buf + 18, END_OF_SESSION);
        return MOLD_HEADER_SIZE;
    }
};

// ─────────────────────────────────────────────
// High-resolution timing
// ─────────────────────────────────────────────

using Clock = std::chrono::steady_clock;
using TimePoint = Clock::time_point;
using Duration = std::chrono::nanoseconds;

static inline void spin_wait_until(TimePoint target) {
    // Busy-spin for tight timing (no sleep/yield)
    while (Clock::now() < target) {
        // spin
    }
}

static inline void sleep_until_approx(TimePoint target) {
    // Sleep for most of the duration, spin for the last ~50μs
    auto now = Clock::now();
    auto remaining = target - now;
    auto sleep_threshold = std::chrono::microseconds(100);

    if (remaining > sleep_threshold) {
        std::this_thread::sleep_for(remaining - sleep_threshold);
    }
    // Spin for the final stretch
    while (Clock::now() < target) {}
}

// ─────────────────────────────────────────────
// Send modes
// ─────────────────────────────────────────────

struct SendStats {
    uint64_t messages_sent = 0;
    uint64_t packets_sent = 0;
    uint64_t next_seq = 1;
    double elapsed_sec = 0;
};

SendStats send_burst(UDPSocket& sock, ITCHCursor& cursor,
                     const Config& cfg) {
    PacketBuilder pkt(cfg.session, cfg.max_pkt);
    SendStats stats;
    stats.next_seq = 1;

    auto start = Clock::now();

    pkt.reset(stats.next_seq);

    while (cursor.has_next()) {
        uint16_t msg_len;
        const uint8_t* body = cursor.next(msg_len);
        if (!body) break;

        if (pkt.msg_count >= cfg.batch || !pkt.try_append(body, msg_len)) {
            // Current packet full — send it
            if (pkt.msg_count > 0) {
                size_t pkt_size = pkt.finalize();
                sock.send(pkt.buf, pkt_size);
                stats.messages_sent += pkt.msg_count;
                stats.packets_sent++;
                stats.next_seq += pkt.msg_count;
            }

            // Start new packet with this message
            pkt.reset(stats.next_seq);
            pkt.try_append(body, msg_len);
        }
    }

    // Send remaining
    if (pkt.msg_count > 0) {
        size_t pkt_size = pkt.finalize();
        sock.send(pkt.buf, pkt_size);
        stats.messages_sent += pkt.msg_count;
        stats.packets_sent++;
        stats.next_seq += pkt.msg_count;
    }

    auto end = Clock::now();
    stats.elapsed_sec = std::chrono::duration<double>(end - start).count();
    return stats;
}

SendStats send_throttled(UDPSocket& sock, ITCHCursor& cursor,
                         const Config& cfg) {
    PacketBuilder pkt(cfg.session, cfg.max_pkt);
    SendStats stats;
    stats.next_seq = 1;

    // Calculate inter-packet delay based on target rate and batch size
    double msgs_per_pkt = static_cast<double>(cfg.batch);
    double pkt_interval_ns = (msgs_per_pkt / cfg.rate) * 1e9;
    auto pkt_delay = Duration(static_cast<int64_t>(pkt_interval_ns));

    auto start = Clock::now();
    auto next_send_time = start;

    pkt.reset(stats.next_seq);

    while (cursor.has_next()) {
        uint16_t msg_len;
        const uint8_t* body = cursor.next(msg_len);
        if (!body) break;

        if (pkt.msg_count >= cfg.batch || !pkt.try_append(body, msg_len)) {
            if (pkt.msg_count > 0) {
                // Wait until send time
                sleep_until_approx(next_send_time);

                size_t pkt_size = pkt.finalize();
                sock.send(pkt.buf, pkt_size);
                stats.messages_sent += pkt.msg_count;
                stats.packets_sent++;
                stats.next_seq += pkt.msg_count;

                next_send_time += pkt_delay;
            }

            pkt.reset(stats.next_seq);
            pkt.try_append(body, msg_len);
        }
    }

    if (pkt.msg_count > 0) {
        sleep_until_approx(next_send_time);
        size_t pkt_size = pkt.finalize();
        sock.send(pkt.buf, pkt_size);
        stats.messages_sent += pkt.msg_count;
        stats.packets_sent++;
        stats.next_seq += pkt.msg_count;
    }

    auto end = Clock::now();
    stats.elapsed_sec = std::chrono::duration<double>(end - start).count();
    return stats;
}

SendStats send_realtime(UDPSocket& sock, ITCHCursor& cursor,
                        const Config& cfg) {
    PacketBuilder pkt(cfg.session, cfg.max_pkt);
    SendStats stats;
    stats.next_seq = 1;

    auto wall_start = Clock::now();

    // Read first message to establish base timestamp
    uint16_t first_len;
    const uint8_t* first_body = cursor.next(first_len);
    if (!first_body) return stats;

    uint64_t base_ts = ITCHCursor::extract_timestamp(first_body, first_len);
    uint64_t prev_ts = base_ts;

    pkt.reset(stats.next_seq);
    pkt.try_append(first_body, first_len);

    while (cursor.has_next()) {
        uint16_t msg_len;
        const uint8_t* body = cursor.next(msg_len);
        if (!body) break;

        uint64_t msg_ts = ITCHCursor::extract_timestamp(body, msg_len);

        // If timestamp changed significantly (>1μs) or batch full, send current packet
        bool ts_changed = (msg_ts > prev_ts) && (msg_ts - prev_ts > 1000);
        bool batch_full = (pkt.msg_count >= cfg.batch);

        if ((ts_changed || batch_full) && pkt.msg_count > 0) {
            // Wait until the appropriate wall-clock time
            double elapsed_ns = static_cast<double>(msg_ts - base_ts) / cfg.speed;
            auto target = wall_start + Duration(static_cast<int64_t>(elapsed_ns));
            sleep_until_approx(target);

            size_t pkt_size = pkt.finalize();
            sock.send(pkt.buf, pkt_size);
            stats.messages_sent += pkt.msg_count;
            stats.packets_sent++;
            stats.next_seq += pkt.msg_count;

            pkt.reset(stats.next_seq);
        }

        pkt.try_append(body, msg_len);
        prev_ts = msg_ts;
    }

    // Send remaining
    if (pkt.msg_count > 0) {
        size_t pkt_size = pkt.finalize();
        sock.send(pkt.buf, pkt_size);
        stats.messages_sent += pkt.msg_count;
        stats.packets_sent++;
        stats.next_seq += pkt.msg_count;
    }

    auto end = Clock::now();
    stats.elapsed_sec = std::chrono::duration<double>(end - wall_start).count();
    return stats;
}

// ─────────────────────────────────────────────
// Argument parsing
// ─────────────────────────────────────────────

void print_usage(const char* prog) {
    fprintf(stderr,
        "Usage: %s <itch_file> [options]\n"
        "\n"
        "Options:\n"
        "  --host <ip>       Destination IP (default: 127.0.0.1)\n"
        "  --port <port>     Destination port (default: 12345)\n"
        "  --mode <mode>     burst | throttled | realtime (default: throttled)\n"
        "  --rate <n>        Messages/sec for throttled mode (default: 100000)\n"
        "  --batch <n>       Max messages per packet (default: 20)\n"
        "  --max-pkt <n>     Max UDP payload bytes (default: 1400)\n"
        "  --session <str>   Session ID, max 10 chars (default: TEST000001)\n"
        "  --speed <x>       Time multiplier for realtime mode (default: 1.0)\n",
        prog);
}

bool parse_args(int argc, char* argv[], Config& cfg) {
    if (argc < 2) {
        print_usage(argv[0]);
        return false;
    }

    cfg.itch_file = argv[1];

    for (int i = 2; i < argc; i++) {
        if (strcmp(argv[i], "--host") == 0 && i + 1 < argc) {
            cfg.host = argv[++i];
        } else if (strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
            cfg.port = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--mode") == 0 && i + 1 < argc) {
            i++;
            if (strcmp(argv[i], "burst") == 0) cfg.mode = SendMode::BURST;
            else if (strcmp(argv[i], "throttled") == 0) cfg.mode = SendMode::THROTTLED;
            else if (strcmp(argv[i], "realtime") == 0) cfg.mode = SendMode::REALTIME;
            else {
                fprintf(stderr, "Unknown mode: %s\n", argv[i]);
                return false;
            }
        } else if (strcmp(argv[i], "--rate") == 0 && i + 1 < argc) {
            cfg.rate = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--batch") == 0 && i + 1 < argc) {
            cfg.batch = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--max-pkt") == 0 && i + 1 < argc) {
            cfg.max_pkt = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--session") == 0 && i + 1 < argc) {
            i++;
            memset(cfg.session, ' ', 10);
            size_t len = strlen(argv[i]);
            if (len > 10) len = 10;
            memcpy(cfg.session, argv[i], len);
        } else if (strcmp(argv[i], "--speed") == 0 && i + 1 < argc) {
            cfg.speed = atof(argv[++i]);
        } else if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
            print_usage(argv[0]);
            return false;
        } else {
            fprintf(stderr, "Unknown option: %s\n", argv[i]);
            print_usage(argv[0]);
            return false;
        }
    }

    return true;
}

const char* mode_str(SendMode m) {
    switch (m) {
        case SendMode::BURST:     return "burst";
        case SendMode::THROTTLED: return "throttled";
        case SendMode::REALTIME:  return "realtime";
    }
    return "unknown";
}

// ─────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────

int main(int argc, char* argv[]) {
    Config cfg;
    if (!parse_args(argc, argv, cfg)) return 1;

    // mmap the ITCH file
    MappedFile file;
    if (!file.open(cfg.itch_file)) return 1;

    printf("Mapped %s (%zu bytes)\n", cfg.itch_file, file.size);

    // Count messages (quick scan)
    {
        ITCHCursor scan(file.data, file.size);
        size_t count = 0;
        size_t total_bytes = 0;
        while (scan.has_next()) {
            uint16_t len;
            const uint8_t* body = scan.next(len);
            if (!body) break;
            count++;
            total_bytes += len;
        }
        printf("  Messages:   %zu\n", count);
        printf("  Data bytes: %zu\n", total_bytes);
    }

    // Open UDP socket
    UDPSocket sock;
    if (!sock.open(cfg.host, cfg.port)) return 1;

    printf("\nSending to %s:%d\n", cfg.host, cfg.port);
    printf("  Session:    %.10s\n", cfg.session);
    printf("  Mode:       %s\n", mode_str(cfg.mode));
    printf("  Max batch:  %d messages/packet\n", cfg.batch);
    printf("  Max packet: %d bytes\n", cfg.max_pkt);
    if (cfg.mode == SendMode::THROTTLED) {
        printf("  Rate:       %d messages/sec\n", cfg.rate);
    }
    if (cfg.mode == SendMode::REALTIME) {
        printf("  Speed:      %.1fx\n", cfg.speed);
    }
    printf("\n");

    // Send
    ITCHCursor cursor(file.data, file.size);
    SendStats stats;

    switch (cfg.mode) {
        case SendMode::BURST:
            stats = send_burst(sock, cursor, cfg);
            break;
        case SendMode::THROTTLED:
            stats = send_throttled(sock, cursor, cfg);
            break;
        case SendMode::REALTIME:
            stats = send_realtime(sock, cursor, cfg);
            break;
    }

    // Send end-of-session packets (3x)
    {
        PacketBuilder pkt(cfg.session, cfg.max_pkt);
        for (int i = 0; i < 3; i++) {
            size_t pkt_size = pkt.build_end_of_session(stats.next_seq);
            sock.send(pkt.buf, pkt_size);
            usleep(100000);  // 100ms between EOS packets
        }
    }

    printf("Done!\n");
    printf("  Messages sent:  %lu\n", stats.messages_sent);
    printf("  Packets sent:   %lu\n", stats.packets_sent);
    printf("  Sequence range: 1 to %lu\n", stats.next_seq - 1);
    printf("  Elapsed:        %.3f s\n", stats.elapsed_sec);
    if (stats.elapsed_sec > 0) {
        printf("  Throughput:     %.0f messages/sec\n",
               stats.messages_sent / stats.elapsed_sec);
    }

    return 0;
}
