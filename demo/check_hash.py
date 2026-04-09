#!/usr/bin/env python3
"""Check jump consistent hash placement for demo keys."""
import struct

def crc32c(data):
    """Pure Python CRC32C."""
    crc = 0xFFFFFFFF
    poly = 0x82F63B78
    for b in data:
        crc ^= b
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ poly
            else:
                crc >>= 1
    return crc ^ 0xFFFFFFFF

def jump_consistent_hash(key, num_buckets):
    b = -1
    j = 0
    while j < num_buckets:
        b = j
        key = (key * 2862933555777941757 + 1) & 0xFFFFFFFFFFFFFFFF
        j = int((b + 1) * (float(1 << 31) / float((key >> 33) + 1)))
    return b

keys = [
    "default/aglTkvHS",
    "default/1234368-1-1.PDF",
    "default/G7qoI9yO",
    "default/1234368-2-1.PDF",
    "default/bB0pV1mq",
    "default/1234368-2.PDF",
    "default/1234368-1.PDF",
    "default/multi.txt",
    "default/obj_0383486",
]

n = 5
rf = 4
for key in keys:
    h = crc32c(key.encode("utf-8"))
    home = jump_consistent_hash(h, n)
    targets = []
    for offset in range(n):
        sid = (home + offset) % n
        targets.append(sid)
        if len(targets) == rf:
            break
    excluded = set(range(n)) - set(targets)
    print(f"key={key:30s}  crc32c=0x{h:08x}  home={home}  targets={targets}  excluded={excluded}")
