#!/usr/bin/env python3
"""
Generate a realistic ITCH 5.0 test binary file.

Wire format per the spec:
- Each message is prefixed with a 2-byte big-endian length (length of message body, NOT including the 2-byte prefix itself)
- All integer fields are big-endian unsigned unless noted
- Timestamps are 6-byte nanoseconds since midnight
- Prices are Price(4): 4-byte unsigned int with 4 implied decimal places (e.g. $150.25 = 1502500)
- Stock symbols are 8 bytes, left-justified, right-padded with spaces
- Stock Locate is a 2-byte integer assigned per symbol via Stock Directory ('R') messages

Message types generated:
  S  - System Event
  R  - Stock Directory
  A  - Add Order (no MPID)
  F  - Add Order (with MPID)
  E  - Order Executed
  C  - Order Executed With Price
  X  - Order Cancel (partial)
  D  - Order Delete
  U  - Order Replace
"""

import struct
import random
import os

# ──────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────

SYMBOLS = [
    b"AAPL    ", b"MSFT    ", b"GOOG    ", b"AMZN    ", b"TSLA    ",
    b"NVDA    ", b"META    ", b"NFLX    ", b"JPM     ", b"V       ",
]

# Base prices in Price(4) format (4 implied decimals)
# e.g. AAPL ~$185 -> 1850000
BASE_PRICES = [
    1850000, 4200000, 1750000, 1850000, 2500000,
    8800000, 5000000, 6200000, 1950000, 2800000,
]

MPIDS = [b"NSDQ", b"GSCO", b"MSCO", b"JPMS", b"CITI", b"UBSS", b"BOFM", b"RBCM"]

NUM_LIVE_UPDATES = 1000

MARKET_OPEN_NS  = 34200 * 10**9   # 09:30:00
MARKET_CLOSE_NS = 57600 * 10**9   # 16:00:00


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────

def ts_bytes(ns: int) -> bytes:
    """Encode a nanosecond timestamp as 6-byte big-endian."""
    return ns.to_bytes(6, "big")


def write_msg(f, body: bytes):
    """Write a length-prefixed ITCH message (2-byte BE length + body)."""
    f.write(struct.pack(">H", len(body)))
    f.write(body)


# ──────────────────────────────────────────────
# Message builders (return raw body bytes)
# ──────────────────────────────────────────────

def msg_system_event(stock_locate: int, tracking: int, timestamp_ns: int, event_code: bytes) -> bytes:
    """S - System Event Message (12 bytes)"""
    return struct.pack(">cHH", b"S", stock_locate, tracking) + ts_bytes(timestamp_ns) + event_code


def msg_stock_directory(stock_locate: int, tracking: int, timestamp_ns: int, stock: bytes) -> bytes:
    """R - Stock Directory Message (39 bytes)
    
    We fill required fields with sensible defaults and pad the rest.
    """
    body = struct.pack(">cHH", b"R", stock_locate, tracking)
    body += ts_bytes(timestamp_ns)
    body += stock                          # 8 bytes - Stock
    body += b"Q"                           # 1 byte  - Market Category (Nasdaq Global Select)
    body += b"N"                           # 1 byte  - Financial Status Indicator (Normal)
    body += struct.pack(">I", 100)         # 4 bytes - Round Lot Size
    body += b"N"                           # 1 byte  - Round Lots Only
    body += b"C"                           # 1 byte  - Issue Classification (Common Stock)
    body += b"Z "                          # 2 bytes - Issue Sub-Type
    body += b"P"                           # 1 byte  - Authenticity (Live/Production)
    body += b"N"                           # 1 byte  - Short Sale Threshold Indicator
    body += b"N"                           # 1 byte  - IPO Flag
    body += b"1"                           # 1 byte  - LULD Reference Price Tier
    body += b"N"                           # 1 byte  - ETP Flag
    body += struct.pack(">I", 0)           # 4 bytes - ETP Leverage Factor
    body += b"N"                           # 1 byte  - Inverse Indicator
    return body


def msg_add_order(stock_locate: int, tracking: int, timestamp_ns: int,
                  order_ref: int, side: bytes, shares: int, stock: bytes, price: int) -> bytes:
    """A - Add Order, No MPID Attribution (36 bytes)"""
    body = struct.pack(">cHH", b"A", stock_locate, tracking)
    body += ts_bytes(timestamp_ns)
    body += struct.pack(">Q", order_ref)   # 8 bytes - Order Reference Number
    body += side                           # 1 byte  - Buy/Sell Indicator
    body += struct.pack(">I", shares)      # 4 bytes - Shares
    body += stock                          # 8 bytes - Stock
    body += struct.pack(">I", price)       # 4 bytes - Price(4)
    return body


def msg_add_order_mpid(stock_locate: int, tracking: int, timestamp_ns: int,
                       order_ref: int, side: bytes, shares: int, stock: bytes, price: int,
                       mpid: bytes) -> bytes:
    """F - Add Order with MPID Attribution (40 bytes)"""
    body = struct.pack(">cHH", b"F", stock_locate, tracking)
    body += ts_bytes(timestamp_ns)
    body += struct.pack(">Q", order_ref)
    body += side
    body += struct.pack(">I", shares)
    body += stock
    body += struct.pack(">I", price)
    body += mpid                           # 4 bytes - Attribution
    return body


def msg_order_executed(stock_locate: int, tracking: int, timestamp_ns: int,
                       order_ref: int, executed_shares: int, match_number: int) -> bytes:
    """E - Order Executed Message (31 bytes)"""
    body = struct.pack(">cHH", b"E", stock_locate, tracking)
    body += ts_bytes(timestamp_ns)
    body += struct.pack(">Q", order_ref)
    body += struct.pack(">I", executed_shares)
    body += struct.pack(">Q", match_number)
    return body


def msg_order_executed_price(stock_locate: int, tracking: int, timestamp_ns: int,
                              order_ref: int, executed_shares: int, match_number: int,
                              printable: bytes, price: int) -> bytes:
    """C - Order Executed With Price Message (36 bytes)"""
    body = struct.pack(">cHH", b"C", stock_locate, tracking)
    body += ts_bytes(timestamp_ns)
    body += struct.pack(">Q", order_ref)
    body += struct.pack(">I", executed_shares)
    body += struct.pack(">Q", match_number)
    body += printable                      # 1 byte  - Printable
    body += struct.pack(">I", price)       # 4 bytes - Execution Price(4)
    return body


def msg_order_cancel(stock_locate: int, tracking: int, timestamp_ns: int,
                     order_ref: int, cancelled_shares: int) -> bytes:
    """X - Order Cancel Message (23 bytes)"""
    body = struct.pack(">cHH", b"X", stock_locate, tracking)
    body += ts_bytes(timestamp_ns)
    body += struct.pack(">Q", order_ref)
    body += struct.pack(">I", cancelled_shares)
    return body


def msg_order_delete(stock_locate: int, tracking: int, timestamp_ns: int,
                     order_ref: int) -> bytes:
    """D - Order Delete Message (19 bytes)"""
    body = struct.pack(">cHH", b"D", stock_locate, tracking)
    body += ts_bytes(timestamp_ns)
    body += struct.pack(">Q", order_ref)
    return body


def msg_order_replace(stock_locate: int, tracking: int, timestamp_ns: int,
                      original_ref: int, new_ref: int, shares: int, price: int) -> bytes:
    """U - Order Replace Message (35 bytes)"""
    body = struct.pack(">cHH", b"U", stock_locate, tracking)
    body += ts_bytes(timestamp_ns)
    body += struct.pack(">Q", original_ref)
    body += struct.pack(">Q", new_ref)
    body += struct.pack(">I", shares)
    body += struct.pack(">I", price)
    return body


# ──────────────────────────────────────────────
# State tracking for realistic generation
# ──────────────────────────────────────────────

class OrderState:
    """Tracks live orders so we only execute/cancel/delete/replace orders that exist."""

    def __init__(self):
        self.next_order_ref = 1001
        self.next_match_num = 5001
        # order_ref -> {stock_locate, side, shares_remaining, price, stock_index}
        self.live_orders: dict[int, dict] = {}

    def alloc_order_ref(self) -> int:
        ref = self.next_order_ref
        self.next_order_ref += 1
        return ref

    def alloc_match_num(self) -> int:
        num = self.next_match_num
        self.next_match_num += 1
        return num

    def add(self, order_ref: int, stock_locate: int, side: bytes, shares: int, price: int, stock_idx: int):
        self.live_orders[order_ref] = {
            "stock_locate": stock_locate,
            "side": side,
            "shares": shares,
            "price": price,
            "stock_idx": stock_idx,
        }

    def has_live_orders(self) -> bool:
        return len(self.live_orders) > 0

    def random_live_ref(self) -> int | None:
        if not self.live_orders:
            return None
        return random.choice(list(self.live_orders.keys()))

    def get(self, ref: int) -> dict | None:
        return self.live_orders.get(ref)

    def remove(self, ref: int):
        self.live_orders.pop(ref, None)

    def reduce_shares(self, ref: int, amount: int):
        order = self.live_orders.get(ref)
        if order:
            order["shares"] -= amount
            if order["shares"] <= 0:
                self.remove(ref)


# ──────────────────────────────────────────────
# Generation logic
# ──────────────────────────────────────────────

def generate_itch_file(output_path: str):
    random.seed(42)  # reproducible output
    state = OrderState()
    tracking = 0

    def next_tracking() -> int:
        nonlocal tracking
        tracking += 1
        return tracking

    with open(output_path, "wb") as f:

        # ── System Event: Start of Messages ──
        write_msg(f, msg_system_event(0, next_tracking(), MARKET_OPEN_NS - 10**9, b"O"))

        # ── Stock Directory for all 10 symbols ──
        for i, sym in enumerate(SYMBOLS):
            stock_locate = i + 1  # 1-indexed
            write_msg(f, msg_stock_directory(stock_locate, next_tracking(), MARKET_OPEN_NS - 5 * 10**8, sym))

        # ── Seed the book: 5 orders per symbol (50 total) ──
        ts = MARKET_OPEN_NS
        for i, sym in enumerate(SYMBOLS):
            stock_locate = i + 1
            base_price = BASE_PRICES[i]

            for j in range(5):
                ref = state.alloc_order_ref()
                side = b"B" if j < 3 else b"S"
                # Bids slightly below base, asks slightly above
                tick_offset = (j + 1) * 1000  # each tick = $0.10
                price = base_price - tick_offset if side == b"B" else base_price + tick_offset
                shares = random.choice([100, 200, 300, 500, 1000])

                ts += random.randint(100, 5000)

                # 30% of adds use MPID attribution (F), 70% use plain (A)
                if random.random() < 0.3:
                    write_msg(f, msg_add_order_mpid(
                        stock_locate, next_tracking(), ts, ref, side, shares, sym, price,
                        random.choice(MPIDS)))
                else:
                    write_msg(f, msg_add_order(
                        stock_locate, next_tracking(), ts, ref, side, shares, sym, price))

                state.add(ref, stock_locate, side, shares, price, i)

        # ── 1000 live updates ──
        time_step = (MARKET_CLOSE_NS - ts) // (NUM_LIVE_UPDATES + 1)

        for update_idx in range(NUM_LIVE_UPDATES):
            ts += time_step + random.randint(-time_step // 4, time_step // 4)
            ts = max(ts, MARKET_OPEN_NS)
            ts = min(ts, MARKET_CLOSE_NS - 10**6)

            # Decide what kind of update to generate
            # Weighted towards adds so the book stays populated
            if not state.has_live_orders() or len(state.live_orders) < 20:
                action = "add"
            else:
                r = random.random()
                if r < 0.35:
                    action = "add"
                elif r < 0.55:
                    action = "execute"
                elif r < 0.70:
                    action = "execute_price"
                elif r < 0.80:
                    action = "cancel"
                elif r < 0.90:
                    action = "delete"
                else:
                    action = "replace"

            if action == "add":
                stock_idx = random.randint(0, len(SYMBOLS) - 1)
                stock_locate = stock_idx + 1
                sym = SYMBOLS[stock_idx]
                base_price = BASE_PRICES[stock_idx]
                ref = state.alloc_order_ref()
                side = random.choice([b"B", b"S"])
                spread = random.randint(1, 50) * 100  # 1-50 ticks of $0.01
                price = base_price - spread if side == b"B" else base_price + spread
                shares = random.choice([100, 200, 300, 500, 1000])

                if random.random() < 0.3:
                    write_msg(f, msg_add_order_mpid(
                        stock_locate, next_tracking(), ts, ref, side, shares, sym, price,
                        random.choice(MPIDS)))
                else:
                    write_msg(f, msg_add_order(
                        stock_locate, next_tracking(), ts, ref, side, shares, sym, price))

                state.add(ref, stock_locate, side, shares, price, stock_idx)

            elif action == "execute":
                ref = state.random_live_ref()
                if ref is None:
                    continue
                order = state.get(ref)
                exec_shares = min(random.choice([50, 100, 100, 200]), order["shares"])
                if exec_shares <= 0:
                    continue
                match_num = state.alloc_match_num()

                write_msg(f, msg_order_executed(
                    order["stock_locate"], next_tracking(), ts,
                    ref, exec_shares, match_num))

                state.reduce_shares(ref, exec_shares)

            elif action == "execute_price":
                ref = state.random_live_ref()
                if ref is None:
                    continue
                order = state.get(ref)
                exec_shares = min(random.choice([50, 100, 100, 200]), order["shares"])
                if exec_shares <= 0:
                    continue
                match_num = state.alloc_match_num()
                # Execute at slightly different price (price improvement)
                price_improvement = random.randint(1, 10) * 100
                exec_price = order["price"] + price_improvement if order["side"] == b"B" else order["price"] - price_improvement

                write_msg(f, msg_order_executed_price(
                    order["stock_locate"], next_tracking(), ts,
                    ref, exec_shares, match_num, b"Y", exec_price))

                state.reduce_shares(ref, exec_shares)

            elif action == "cancel":
                ref = state.random_live_ref()
                if ref is None:
                    continue
                order = state.get(ref)
                cancel_shares = min(random.choice([50, 100]), order["shares"])
                if cancel_shares <= 0:
                    continue

                write_msg(f, msg_order_cancel(
                    order["stock_locate"], next_tracking(), ts,
                    ref, cancel_shares))

                state.reduce_shares(ref, cancel_shares)

            elif action == "delete":
                ref = state.random_live_ref()
                if ref is None:
                    continue
                order = state.get(ref)

                write_msg(f, msg_order_delete(
                    order["stock_locate"], next_tracking(), ts, ref))

                state.remove(ref)

            elif action == "replace":
                ref = state.random_live_ref()
                if ref is None:
                    continue
                order = state.get(ref)
                new_ref = state.alloc_order_ref()
                base_price = BASE_PRICES[order["stock_idx"]]
                spread = random.randint(1, 50) * 100
                new_price = base_price - spread if order["side"] == b"B" else base_price + spread
                new_shares = random.choice([100, 200, 300, 500, 1000])

                write_msg(f, msg_order_replace(
                    order["stock_locate"], next_tracking(), ts,
                    ref, new_ref, new_shares, new_price))

                # Replace = delete old + add new (same side/symbol)
                state.remove(ref)
                state.add(new_ref, order["stock_locate"], order["side"],
                          new_shares, new_price, order["stock_idx"])

        # ── System Event: End of Messages ──
        write_msg(f, msg_system_event(0, next_tracking(), MARKET_CLOSE_NS, b"C"))

    file_size = os.path.getsize(output_path)
    print(f"Generated {output_path}")
    print(f"  File size:    {file_size:,} bytes")
    print(f"  Symbols:      {len(SYMBOLS)}")
    print(f"  Seed orders:  50 (5 per symbol)")
    print(f"  Live updates: {NUM_LIVE_UPDATES}")
    print(f"  Final live orders: {len(state.live_orders)}")
    print(f"  Order refs used:   {state.next_order_ref - 1001}")
    print(f"  Match nums used:   {state.next_match_num - 5001}")


# ──────────────────────────────────────────────
# Verification: parse the file back and print summary
# ──────────────────────────────────────────────

MSG_SIZES = {
    ord("S"): 12, ord("R"): 39, ord("A"): 36, ord("F"): 40,
    ord("E"): 31, ord("C"): 36, ord("X"): 23, ord("D"): 19, ord("U"): 35,
}

MSG_NAMES = {
    ord("S"): "SystemEvent", ord("R"): "StockDirectory",
    ord("A"): "AddOrder", ord("F"): "AddOrderMPID",
    ord("E"): "OrderExecuted", ord("C"): "OrderExecPrice",
    ord("X"): "OrderCancel", ord("D"): "OrderDelete", ord("U"): "OrderReplace",
}

def verify_itch_file(path: str):
    counts: dict[int, int] = {}
    total = 0

    with open(path, "rb") as f:
        while True:
            length_bytes = f.read(2)
            if len(length_bytes) < 2:
                break

            msg_len = struct.unpack(">H", length_bytes)[0]
            body = f.read(msg_len)
            if len(body) < msg_len:
                print(f"  WARNING: truncated message at offset {f.tell()}")
                break

            msg_type = body[0]
            counts[msg_type] = counts.get(msg_type, 0) + 1
            total += 1

            # Verify body length matches expected
            expected = MSG_SIZES.get(msg_type)
            if expected and len(body) != expected:
                print(f"  WARNING: msg type {chr(msg_type)} expected {expected} bytes, got {len(body)}")

    print(f"\nVerification of {path}:")
    print(f"  Total messages: {total}")
    for msg_type, count in sorted(counts.items()):
        name = MSG_NAMES.get(msg_type, f"Unknown(0x{msg_type:02x})")
        print(f"    {chr(msg_type)} ({name}): {count}")


if __name__ == "__main__":
    output = "test_itch.bin"
    generate_itch_file(output)
    verify_itch_file(output)
