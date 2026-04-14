# MoldUDP64 Sender

## Official Document

- More information can be found on the link below.
- **Link**: https://www.nasdaqtrader.com/content/technicalsupport/specifications/dataproducts/moldudp64.pdf

## Overview

MoldUDP64 is a thin framing layer on top of regular UDP, designed by NASDAQ for delivering market data feeds. The underlying transport is still plain UDP — same sockets, same unreliable delivery. MoldUDP64 adds three things that raw UDP lacks: **sequencing** (detect lost packets), **message batching** (pack multiple ITCH messages into one UDP datagram), and **session management** (heartbeats and end-of-session signals).

MoldUDP64 does NOT add reliability. If a packet is lost, it's lost. It gives you the tools to *detect* the loss via sequence number gaps, and optionally request retransmission via a separate channel.

## Packet Format

Every MoldUDP64 packet starts with a fixed 20-byte header, followed by zero or more message blocks packed back-to-back.

```
┌──────────────────────────────────────────────────────────┐
│ Session          (10 bytes, ASCII, right-padded spaces)  │
│ Sequence Number  (8 bytes, big-endian uint64)            │
│ Message Count    (2 bytes, big-endian uint16)            │
├──────────────────────────────────────────────────────────┤
│ Message Block 1: [2-byte BE length][message data]        │
│ Message Block 2: [2-byte BE length][message data]        │
│ ...                                                      │
└──────────────────────────────────────────────────────────┘
```

### Header Fields

| Offset | Length | Field | Description |
|--------|--------|-------|-------------|
| 0 | 10 | Session | Identifies the logical stream. Assigned per trading day. |
| 10 | 8 | Sequence Number | Sequence number of the **first** message in this packet. |
| 18 | 2 | Message Count | Number of message blocks. `0` = heartbeat, `0xFFFF` = end of session. |

### Message Blocks

Each block is a 2-byte big-endian length followed by the message body (an ITCH message). Blocks start immediately after the header at byte 20, and subsequent blocks follow with no gaps.

### Special Packets

- **Heartbeat**: Message count = `0`, no message blocks. Sent periodically so receivers can detect link failures. Contains the next expected sequence number.
- **End of Session**: Message count = `0xFFFF`. Signals that no more messages will be sent on this session.

## Sequencing

The sequence number in each packet refers to the first message it contains. Subsequent messages are implicitly numbered. For example:

```
Packet 1:  seq=1,   count=20  →  messages 1-20
Packet 2:  seq=21,  count=15  →  messages 21-35
Packet 3:  seq=36,  count=8   →  messages 36-43
```

If the receiver gets packet 1 (seq=1, count=20) then packet 3 (seq=36), it knows packet 2 was lost because it expected seq=21 but got seq=36. Without this, a lost UDP packet would silently corrupt the order book.

## Why Batching?

Each UDP datagram has 28 bytes of IP+UDP header overhead. ITCH messages are small (19-40 bytes each). Sending each message as a separate UDP packet would mean the headers are larger than the payload. Batching 20 messages into one packet reduces overhead by 20x and reduces system call count (one `sendto` instead of twenty).

## Example Packet

Three ITCH messages in one packet (System Event + Add Order + Order Executed), starting at sequence number 47:

```
Offset    Raw Bytes                          Field
───────   ──────────────────────────────     ─────────────────────────
          ┌─── Header (20 bytes) ──────────────────────────────────┐
0-9       54 45 53 54 30 30 30 30 30 31      Session = "TEST000001"
10-17     00 00 00 00 00 00 00 2F            Sequence Number = 47
18-19     00 03                              Message Count = 3
          └────────────────────────────────────────────────────────┘

          ┌─── Message Block 1 ────────────┐
20-21     00 0C                              Length = 12
22-33     53 00 00 00 01 ...                 ITCH System Event (12 bytes)
          └────────────────────────────────┘

          ┌─── Message Block 2 ────────────┐
34-35     00 24                              Length = 36
36-71     41 00 01 00 0B ...                 ITCH Add Order (36 bytes)
          └────────────────────────────────┘

          ┌─── Message Block 3 ────────────┐
72-73     00 1F                              Length = 31
74-104    45 00 01 00 0C ...                 ITCH Order Executed (31 bytes)
          └────────────────────────────────┘

Total UDP payload: 20 + (2+12) + (2+36) + (2+31) = 105 bytes
```

## Sender

The sender reads an ITCH binary file via `mmap` (zero-copy), wraps messages in MoldUDP64 packets, and sends them over UDP to `127.0.0.1:12345`.

### Build

```bash
g++ -O3 -std=c++20 -Wall -o sender sender.cpp
```

### Usage

```bash
./sender <itch_file> burst                # Send as fast as possible
./sender <itch_file> throttled [rate]     # Send at rate msg/sec (default: 100000)
./sender <itch_file> realtime [speed]     # Replay at original timestamps (default: 1000x)
```

### Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| `burst` | No pacing, sends everything immediately | Stress testing, throughput benchmarking |
| `throttled` | Fixed rate with configurable msg/sec | Controlled latency measurement |
| `realtime` | Sleeps based on ITCH timestamp deltas, with speed multiplier | Simulating a real trading day |

### Defaults

| Parameter | Value |
|-----------|-------|
| Host | `127.0.0.1` |
| Port | `12345` |
| Max batch | 20 messages per packet |
| Max packet | 1400 bytes (conservative, below 1500 Ethernet MTU) |
| Session ID | `TEST000001` |
