// include/spsc_queue.h
#pragma once
#include <atomic>
#include <cstdint>
#include <cstring>

struct alignas(64) Packet {
    uint16_t len;
    uint8_t data[1500];
};

struct SPSCQueue {
    static constexpr size_t CAPACITY = 65536;  // must be power of two
    static constexpr size_t MASK = 0xFFFF;

    // Each on its own cache line to prevent false sharing
    // Producer only writes write_idx, consumer only writes read_idx
    // Without this padding, both atomics share a cache line and every
    // write by one thread invalidates the other thread's cache
    // Aligned 64 bytes for CPU cache alignment
    alignas(64) std::atomic<uint64_t> write_idx{0};
    alignas(64) std::atomic<uint64_t> read_idx{0};
    alignas(64) Packet slots[CAPACITY];
    
    // Use relaxed for own index, acquire/release when for other's index
    // When reading other's index, gotta see the all data that thread wrote
    // relaxed
    // - Just push whatever, only atomicity guaranteed
    // release & acqure:
    // - Everything before release-store is visible to others 
    //   who acquire-loads this value
    // 
    // 1. Reading own index -> relaxed
    // 2. Reading other's index: acquire-load
    // 3. Advancing my index: release-store

    // Called by receiver thread (producer)
    bool try_push(const uint8_t* data, uint16_t len) {
        uint64_t w = write_idx.load(std::memory_order_relaxed);
        uint64_t r = read_idx.load(std::memory_order_acquire);

        if (w - r >= CAPACITY) return false;  // full

        auto& slot = slots[w & MASK];
        slot.len = len;
        memcpy(slot.data, data, len);

        // Release ensures the memcpy is visible before the index advances
        write_idx.store(w + 1, std::memory_order_release);
        return true;
    }

    // Called by book handler thread (consumer)
    const Packet* try_peek() {
        uint64_t r = read_idx.load(std::memory_order_relaxed);
        uint64_t w = write_idx.load(std::memory_order_acquire);

        if (r >= w) return nullptr;  // empty
        return &slots[r & MASK];
    }

    void pop() {
        uint64_t r = read_idx.load(std::memory_order_relaxed);
        read_idx.store(r + 1, std::memory_order_release);
    }
};
