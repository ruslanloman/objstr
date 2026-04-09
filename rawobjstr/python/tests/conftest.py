"""Shared fixtures and helpers for rawobjstr tests."""

import struct

import pytest

import rawobjstr

# ---------------------------------------------------------------------------
# Constants (match tests/common/mod.rs)
# ---------------------------------------------------------------------------

SMALL_DEVICE = 64 * 1024 * 1024       # 64 MB
MEDIUM_DEVICE = 256 * 1024 * 1024     # 256 MB
CHUNK_SIZE = 1024 * 1024              # 1 MB

# DATA_START must match the Rust constant.  It is the byte offset where
# the first extent is written: SUPERBLOCK_SIZE (4096) * 2 = 8192.
# If the Rust crate changes this, the test will fail with a clear message.
DATA_START = 8192


# ---------------------------------------------------------------------------
# Payload helpers (match make_chunk / verify_chunk / make_small / verify_small)
# ---------------------------------------------------------------------------

def make_chunk(index):
    """Deterministic 1 MB payload: first 8 bytes = index as LE u64, rest filled."""
    buf = bytearray(CHUNK_SIZE)
    struct.pack_into("<Q", buf, 0, index)
    fill = index & 0xFF
    for i in range(8, CHUNK_SIZE):
        buf[i] = fill
    return bytes(buf)


def verify_chunk(data, expected_index):
    assert len(data) == CHUNK_SIZE, f"chunk {expected_index} wrong size"
    stored = struct.unpack_from("<Q", data, 0)[0]
    assert stored == expected_index, "chunk tag mismatch"
    fill = expected_index & 0xFF
    for i in range(8, CHUNK_SIZE):
        assert data[i] == fill, f"chunk {expected_index} byte {i} mismatch"


def make_small(index, size):
    """Deterministic payload of arbitrary size."""
    fill = index & 0xFF
    buf = bytearray([fill] * size)
    if size >= 8:
        struct.pack_into("<Q", buf, 0, index)
    return bytes(buf)


def verify_small(data, index, expected_size):
    assert len(data) == expected_size, f"index {index}: wrong size"
    if len(data) >= 8:
        tag = struct.unpack_from("<Q", data, 0)[0]
        assert tag == index, f"index {index}: tag mismatch (got {tag})"
        fill = index & 0xFF
        for j in range(8, len(data)):
            assert data[j] == fill, f"index {index}: corrupt at byte {j}"


def make_payload(size):
    """Deterministic payload: byte[i] = i % 251."""
    return bytes(i % 251 for i in range(size))


def flip_byte(path, offset):
    """Flip one byte on disk to simulate corruption."""
    with open(path, "r+b") as f:
        f.seek(offset)
        b = f.read(1)
        f.seek(offset)
        f.write(bytes([b[0] ^ 0xFF]))


def compressible_payload(size):
    """Highly compressible: repeated pattern."""
    pattern = b"The quick brown fox jumps over the lazy dog. "
    reps = (size // len(pattern)) + 1
    return (pattern * reps)[:size]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session", autouse=True)
def session_build_info():
    """Capture build info at session start and verify it is unchanged at the end."""
    build_info = rawobjstr.__build_info__
    print(f"\n[rawobjstr] build: {build_info}")
    yield build_info
    current = rawobjstr.__build_info__
    assert current == build_info, (
        f"rawobjstr build info changed during test session!\n"
        f"  start: {build_info}\n"
        f"  end:   {current}"
    )


@pytest.fixture
def store_path(tmp_path):
    return str(tmp_path / "test_store.raw")


@pytest.fixture
def store(store_path):
    s = rawobjstr.format(store_path, size=SMALL_DEVICE)
    yield s
    s.flush_index()


@pytest.fixture
def medium_store(tmp_path):
    p = str(tmp_path / "medium_store.raw")
    s = rawobjstr.format(p, size=MEDIUM_DEVICE)
    yield s
    s.flush_index()
