# feed-handler

Parses ITCH binary data and tracks positions incredibly fast using various tricks

## Overview

Offset  Length  Field             Description
0       10      Session           ASCII session ID, right-padded with spaces
10      8       Sequence Number   Big-endian uint64, sequence of first message in this packet
18      2       Message Count     Big-endian uint16, number of messages in this packet
                                    0      = heartbeat (no messages, just "I'm alive")
                                    0xFFFF = end of session
                                    other  = number of message blocks following
