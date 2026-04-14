# Test Files

## Official Document

- More information can be found on the link below.
- **Link**: https://www.nasdaqtrader.com/content/technicalsupport/specifications/dataproducts/NQTVITCHSpecification.pdf
- Real NASDAQ test file (**WARNING: Huge**)
    - https://emi.nasdaq.com/ITCH/Nasdaq%20ITCH/

## Overview

ITCH 5.0 is NASDAQ's binary market data feed protocol. It delivers a stream of messages describing every order added, modified, and executed on the exchange. It is a **read-only outbound feed**, meaning you receive it to reconstruct the order book, you don't send orders through it (that's OUCH).

## Wire Format

Each message is prefixed with a **2-byte big-endian length**, followed by the message body. Messages are packed back-to-back with no delimiters.

```
[2-byte length][message body][2-byte length][message body]...
```

All integers are **big-endian unsigned**. Timestamps are **6-byte nanoseconds since midnight**. Stock symbols are **8 bytes, right-padded with spaces**. Prices use **4 implied decimal places** (e.g. `1501234` = `$150.1234`).

## Stock Locate

Every message contains a `stock_locate` field (2-byte integer) instead of the full symbol. The mapping from `stock_locate` -> symbol is established at the start of the day via Stock Directory (`R`) messages. This saves 6 bytes per message across hundreds of millions of messages.

### Message Types (Book-Affecting)

| Type | Name | Size | Description |
|------|------|------|-------------|
| `S` | System Event | 12 | Market open/close signals |
| `R` | Stock Directory | 39 | Establishes `stock_locate` -> symbol mapping |
| `A` | Add Order | 36 | New order on the book (no attribution) |
| `F` | Add Order MPID | 40 | New order with market participant ID |
| `E` | Order Executed | 31 | Partial/full fill at original price |
| `C` | Order Executed w/ Price | 36 | Fill at a different price (price improvement) |
| `X` | Order Cancel | 23 | Partial cancellation (reduce shares) |
| `D` | Order Delete | 19 | Full cancellation (remove order) |
| `U` | Order Replace | 35 | Cancel-replace: deletes old order, creates new one with new ref/price/size |

### Key Design Notes

- **`E`/`C`/`X`/`D`/`U` messages do NOT carry the stock symbol or side.** You must look these up from the original `A`/`F` add message via the order reference number.
- **`U` (Replace)** assigns a new order reference number. The old ref is dead; all subsequent messages use the new ref. Side, symbol, and MPID carry over from the original add.
- **`X` (Cancel) vs `D` (Delete):** Cancel is partial (reduce by N shares), Delete is full (remove entirely).
- **`P`/`Q`/`B` (Trade messages)** exist in the protocol but do NOT affect the displayable book and can be skipped for book reconstruction.

### Test File: `test_itch.bin`

Contains 10 symbols, 50 seed orders, and 1000 live updates covering all book-affecting message types. Run the generator to regenerate or adjust parameters.

### Example ITCH Message

```
Offset  Length  Field                    Value              Raw Bytes
──────  ──────  ─────────────────────    ───────────────    ──────────────────
                [2-byte length prefix]   36                 00 24
0       1       Message Type             'A'                41
1       2       Stock Locate             1                  00 01
3       2       Tracking Number          11                 00 0B
5       6       Timestamp (ns)           34200000001753     00 1F 1A B1 0C D9
11      8       Order Reference Number   1001               00 00 00 00 00 00 03 E9
19      1       Buy/Sell Indicator        'B'               42
20      4       Shares                   100                00 00 00 64
24      8       Stock                    "AAPL    "         41 41 50 4C 20 20 20 20
32      4       Price                    1849000            00 1C 33 28
```
