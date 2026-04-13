#!/usr/bin/env python3
"""
MoldUDP64 sender for ITCH test data.

Reads an ITCH binary file (length-prefixed messages) and sends them
over UDP wrapped in MoldUDP64 packets.

MoldUDP64 packet format (20-byte header + message blocks):
┌──────────────────────────────────────────────────┐
│ Session (10 bytes, ASCII, right-padded spaces)   │
│ Sequence Number (8 bytes, big-endian uint64)     │
│ Message Count (2 bytes, big-endian uint16)       │
├──────────────────────────────────────────────────┤
│ Message Block 1: [2-byte length][message data]   │
│ Message Block 2: [2-byte length][message data]   │
│ ...                                              │
└──────────────────────────────────────────────────┘

Usage:
    python3 moldudp_sender.py <itch_file> [options]

Options:
    --host          Destination IP (default: 127.0.0.1)
    --port          Destination port (default: 12345)
    --mode          Send mode: burst | throttled | realtime (default: throttled)
    --rate          Messages per second for throttled mode (default: 10000)
    --batch         Max messages per MoldUDP64 packet (default: 20)
    --max-pkt-size  Max UDP payload size in bytes (default: 1400)
    --session       Session ID string, max 10 chars (default: "TEST000001")
    --heartbeat     Heartbeat interval in seconds (default: 1.0)
"""

import argparse
import socket
import struct
import time
import sys
import os


# MoldUDP64 constants
HEADER_SIZE = 20          # 10 (session) + 8 (seq_num) + 2 (msg_count)
END_OF_SESSION = 0xFFFF

# ITCH timestamp parsing (for realtime mode)
def extract_timestamp_ns(msg_body: bytes) -> int | None:
    """Extract the 6-byte timestamp from an ITCH message.
    
    Timestamp is at offset 5 in the message body 
    (after msg_type[1] + stock_locate[2] + tracking_number[2]).
    """
    if len(msg_body) < 11:
        return None
    return int.from_bytes(msg_body[5:11], "big")

# File reader: parse length-prefixed ITCH messages
def read_itch_messages(filepath: str) -> list[bytes]:
    """Read all length-prefixed ITCH messages from a binary file.
    
    Each message in the file is: [2-byte BE length][message body]
    We return the raw message bodies (without the length prefix,
    since MoldUDP64 adds its own length prefix).
    """
    messages = []
    with open(filepath, "rb") as f:
        while True:
            length_bytes = f.read(2)
            if len(length_bytes) < 2:
                break
            msg_len = struct.unpack(">H", length_bytes)[0]
            body = f.read(msg_len)
            if len(body) < msg_len:
                print(f"WARNING: truncated message at end of file", file=sys.stderr)
                break
            messages.append(body)
    return messages

# MoldUDP64 packet builder

def build_moldudp64_header(session: bytes, seq_num: int, msg_count: int) -> bytes:
    """Build a 20-byte MoldUDP64 header."""
    return session + struct.pack(">QH", seq_num, msg_count)

def build_moldudp64_packet(session: bytes, seq_num: int, messages: list[bytes]) -> bytes:
    """Build a complete MoldUDP64 packet with header + message blocks.
    
    Each message block is: [2-byte BE length][message data]
    """
    header = build_moldudp64_header(session, seq_num, len(messages))
    payload = b""
    for msg in messages:
        payload += struct.pack(">H", len(msg)) + msg
    return header + payload

def build_heartbeat(session: bytes, next_seq_num: int) -> bytes:
    """Build a MoldUDP64 heartbeat packet (message count = 0)."""
    return build_moldudp64_header(session, next_seq_num, 0)

def build_end_of_session(session: bytes, next_seq_num: int) -> bytes:
    """Build a MoldUDP64 end-of-session packet (message count = 0xFFFF)."""
    return build_moldudp64_header(session, next_seq_num, END_OF_SESSION)


# Batching: group messages into MoldUDP64 packets
def batch_messages(messages: list[bytes], max_batch: int, max_pkt_size: int) -> list[list[bytes]]:
    """Group messages into batches that fit within packet size limits.
    
    Each batch becomes one MoldUDP64 packet. We respect both the
    max messages per packet and the max UDP payload size.
    """
    batches = []
    current_batch = []
    current_size = HEADER_SIZE  # start with header overhead

    for msg in messages:
        msg_block_size = 2 + len(msg)  # 2-byte length prefix + body

        # Would adding this message exceed limits?
        if (len(current_batch) >= max_batch or
                current_size + msg_block_size > max_pkt_size):
            if current_batch:
                batches.append(current_batch)
            current_batch = [msg]
            current_size = HEADER_SIZE + msg_block_size
        else:
            current_batch.append(msg)
            current_size += msg_block_size

    if current_batch:
        batches.append(current_batch)

    return batches


# ──────────────────────────────────────────────
# Sender modes
# ──────────────────────────────────────────────

def send_burst(sock, dest, session: bytes, batches: list[list[bytes]]):
    """Send all packets as fast as possible. No pacing."""
    seq_num = 1
    for batch in batches:
        pkt = build_moldudp64_packet(session, seq_num, batch)
        sock.sendto(pkt, dest)
        seq_num += len(batch)

    return seq_num


def send_throttled(sock, dest, session: bytes, batches: list[list[bytes]],
                   rate: int, heartbeat_interval: float):
    """Send at a fixed message rate with heartbeats during gaps."""
    seq_num = 1
    total_msgs = sum(len(b) for b in batches)

    # Calculate delay between packets based on target message rate
    # and average messages per batch
    avg_batch_size = total_msgs / len(batches) if batches else 1
    pkt_delay = avg_batch_size / rate  # seconds between packets

    last_send_time = time.monotonic()
    last_heartbeat_time = last_send_time

    for i, batch in enumerate(batches):
        pkt = build_moldudp64_packet(session, seq_num, batch)
        sock.sendto(pkt, dest)
        seq_num += len(batch)

        now = time.monotonic()
        last_send_time = now

        # Pace sends
        target_time = last_send_time + pkt_delay
        sleep_time = target_time - time.monotonic()
        if sleep_time > 0:
            time.sleep(sleep_time)

        # Progress
        if (i + 1) % 100 == 0:
            elapsed = time.monotonic() - last_send_time + 0.001
            print(f"  Sent {i+1}/{len(batches)} packets, "
                  f"seq={seq_num}, "
                  f"~{len(batch)/elapsed:.0f} msg/s actual", end="\r")

    return seq_num


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="MoldUDP64 ITCH sender")
    parser.add_argument("itch_file", help="Path to ITCH binary file")
    parser.add_argument("--host", default="127.0.0.1", help="Destination IP (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=12345, help="Destination port (default: 12345)")
    parser.add_argument("--mode", choices=["burst", "throttled", "realtime"], default="throttled",
                        help="Send mode (default: throttled)")
    parser.add_argument("--rate", type=int, default=10000,
                        help="Target messages/sec for throttled mode (default: 10000)")
    parser.add_argument("--batch", type=int, default=20,
                        help="Max messages per MoldUDP64 packet (default: 20)")
    parser.add_argument("--max-pkt-size", type=int, default=1400,
                        help="Max UDP payload size in bytes (default: 1400)")
    parser.add_argument("--session", default="TEST000001",
                        help="Session ID, max 10 chars (default: TEST000001)")
    parser.add_argument("--heartbeat", type=float, default=1.0,
                        help="Heartbeat interval in seconds (default: 1.0)")
    args = parser.parse_args()

    # Validate session ID
    session = args.session.encode("ascii")[:10].ljust(10, b" ")

    # Read ITCH messages
    print(f"Reading {args.itch_file}...")
    messages = read_itch_messages(args.itch_file)
    print(f"  Loaded {len(messages)} ITCH messages")
    print(f"  Total data: {sum(len(m) for m in messages):,} bytes")

    # Create UDP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    dest = (args.host, args.port)

    print(f"\nSending to {args.host}:{args.port}")
    print(f"  Session:    {session.decode('ascii')}")
    print(f"  Mode:       {args.mode}")
    print(f"  Max batch:  {args.batch} messages/packet")
    print(f"  Max packet: {args.max_pkt_size} bytes")
    if args.mode == "throttled":
        print(f"  Rate:       {args.rate} messages/sec")
    print()

    start_time = time.monotonic()

    if args.mode == "burst":
        batches = batch_messages(messages, args.batch, args.max_pkt_size)
        print(f"  Batched into {len(batches)} MoldUDP64 packets")
        next_seq = send_burst(sock, dest, session, batches)

    elif args.mode == "throttled":
        batches = batch_messages(messages, args.batch, args.max_pkt_size)
        print(f"  Batched into {len(batches)} MoldUDP64 packets")
        next_seq = send_throttled(sock, dest, session, batches, args.rate, args.heartbeat)

    elapsed = time.monotonic() - start_time

    # Send end-of-session packets (3x as per convention)
    for _ in range(3):
        sock.sendto(build_end_of_session(session, next_seq), dest)
        time.sleep(0.1)

    print(f"\nDone!")
    print(f"  Messages sent:  {len(messages)}")
    print(f"  Sequence range: 1 to {next_seq - 1}")
    print(f"  Elapsed:        {elapsed:.3f}s")
    if elapsed > 0:
        print(f"  Throughput:     {len(messages)/elapsed:,.0f} messages/sec")

    sock.close()


if __name__ == "__main__":
    main()
