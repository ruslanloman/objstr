"""Image upload with thumbnail metadata -- shows flexible binary metadata.

rawobjstr metadata is an opaque byte suffix (up to 65 535 bytes) stored
alongside each object.  This example uses it to keep a JPEG thumbnail
right next to the full-resolution image -- no extra keys, no sidecar
files, no second round-trip.

Requires Pillow:
    pip install Pillow

Run:
    pytest tests/examples/test_image_metadata.py -v -m examples
"""

import io
import json
import struct

import pytest

import rawobjstr

PIL = pytest.importorskip("PIL")
from PIL import Image  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

STORE_SIZE = 100 * 1024 * 1024  # 100 MB


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_test_image(width: int, height: int) -> bytes:
    """Create a colorful synthetic PNG image and return its bytes."""
    img = Image.new("RGB", (width, height))
    pixels = img.load()
    for x in range(width):
        for y in range(height):
            pixels[x, y] = (x % 256, y % 256, (x + y) % 256)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def _make_thumbnail(image_bytes: bytes, size: tuple) -> bytes:
    """Resize *image_bytes* to *size* and return a JPEG thumbnail."""
    img = Image.open(io.BytesIO(image_bytes))
    img.thumbnail(size)
    buf = io.BytesIO()
    img.save(buf, format="JPEG")
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def store_path(tmp_path):
    """Format a fresh 100 MB store image and return its path."""
    path = str(tmp_path / "images.raw")
    store = rawobjstr.format(path, size=STORE_SIZE)
    store.flush_index()
    del store
    return path


@pytest.fixture
def store(store_path):
    """Open the store for read/write access."""
    s = rawobjstr.open(store_path)
    yield s
    s.flush_index()


# ---------------------------------------------------------------------------
# Examples
# ---------------------------------------------------------------------------

@pytest.mark.examples
class TestThumbnailAsMetadata:
    """Store a JPEG thumbnail in the metadata suffix of a full-size image."""

    def test_upload_image_with_thumbnail(self, store):
        """Put a PNG image with a 64x64 JPEG thumbnail as metadata."""
        full_image = _make_test_image(256, 256)
        thumbnail = _make_thumbnail(full_image, (64, 64))

        # Body = full image, metadata = thumbnail bytes
        store.put_with_meta("photos/sunset.png", full_image, thumbnail)

        # get() returns the full image only -- metadata is stripped
        retrieved = store.get("photos/sunset.png")
        assert retrieved == full_image

        # get_metadata() returns the raw thumbnail bytes
        retrieved_thumb = store.get_metadata("photos/sunset.png")
        assert retrieved_thumb == thumbnail

        # The thumbnail is a valid JPEG -- open it with Pillow
        thumb_img = Image.open(io.BytesIO(retrieved_thumb))
        assert thumb_img.format == "JPEG"
        assert max(thumb_img.size) <= 64

    def test_head_reports_correct_sizes(self, store):
        """head_with_meta separates body size from metadata length."""
        full_image = _make_test_image(200, 150)
        thumbnail = _make_thumbnail(full_image, (48, 48))

        store.put_with_meta("photos/beach.png", full_image, thumbnail)

        obj_meta, meta_len = store.head_with_meta("photos/beach.png")

        # obj_meta.size is the full image size only (no thumbnail)
        assert obj_meta.size == len(full_image)
        # meta_len is the thumbnail byte count
        assert meta_len == len(thumbnail)

    def test_update_thumbnail_without_reupload(self, store):
        """Replace the thumbnail with a smaller one -- body is untouched."""
        full_image = _make_test_image(300, 300)
        thumb_64 = _make_thumbnail(full_image, (64, 64))

        store.put_with_meta("photos/forest.png", full_image, thumb_64)

        # Swap to a 32x32 thumbnail without re-uploading the 300x300 image
        thumb_32 = _make_thumbnail(full_image, (32, 32))
        store.update_metadata("photos/forest.png", thumb_32)

        # Body unchanged
        assert store.get("photos/forest.png") == full_image

        # Metadata is now the 32x32 thumbnail
        new_thumb = store.get_metadata("photos/forest.png")
        assert new_thumb == thumb_32
        thumb_img = Image.open(io.BytesIO(new_thumb))
        assert max(thumb_img.size) <= 32

    def test_list_objects_with_meta_lengths(self, store):
        """list_with_meta shows which objects carry metadata."""
        img_a = _make_test_image(100, 100)
        img_b = _make_test_image(80, 80)
        thumb_a = _make_thumbnail(img_a, (32, 32))

        store.put_with_meta("gallery/a.png", img_a, thumb_a)
        store.put("gallery/b.png", img_b)  # no thumbnail

        results = store.list_with_meta("gallery/")
        by_key = {om.location: ml for om, ml in results}

        assert by_key["gallery/a.png"] == len(thumb_a)
        assert by_key["gallery/b.png"] == 0


@pytest.mark.examples
class TestStructuredMetadata:
    """Pack a JSON header + thumbnail together in one metadata blob.

    Layout:
        [4 bytes: header_len as little-endian u32]
        [header_len bytes: UTF-8 JSON]
        [remaining bytes: JPEG thumbnail]

    This shows you can combine structured data with binary data in the
    same metadata suffix -- the format is entirely up to you.
    """

    @staticmethod
    def _pack_meta(header: dict, thumbnail: bytes) -> bytes:
        header_bytes = json.dumps(header, separators=(",", ":")).encode()
        return struct.pack("<I", len(header_bytes)) + header_bytes + thumbnail

    @staticmethod
    def _unpack_meta(raw: bytes):
        (header_len,) = struct.unpack("<I", raw[:4])
        header = json.loads(raw[4 : 4 + header_len])
        thumbnail = raw[4 + header_len :]
        return header, thumbnail

    def test_json_plus_thumbnail(self, store):
        """Store image dimensions and content-type alongside the thumbnail."""
        full_image = _make_test_image(400, 300)
        thumbnail = _make_thumbnail(full_image, (64, 48))

        header = {
            "content_type": "image/png",
            "width": 400,
            "height": 300,
            "thumb_width": 64,
            "thumb_height": 48,
        }
        metadata = self._pack_meta(header, thumbnail)
        assert len(metadata) < 65535  # must fit in u16

        store.put_with_meta("catalog/photo.png", full_image, metadata)

        # Round-trip: unpack the metadata back out
        raw_meta = store.get_metadata("catalog/photo.png")
        got_header, got_thumb = self._unpack_meta(raw_meta)

        assert got_header == header
        assert got_thumb == thumbnail

        # The thumbnail is still a valid image
        thumb_img = Image.open(io.BytesIO(got_thumb))
        assert thumb_img.format == "JPEG"
