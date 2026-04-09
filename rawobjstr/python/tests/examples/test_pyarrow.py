"""PyArrow + rawobjstr fsspec example.

Shows how to save and load Arrow tables, Parquet files, and CSV files
on a rawobjstr store using PyArrow directly.

Requirements:
    pip install fsspec pyarrow

Run:
    pytest tests/examples/test_pyarrow.py -v -m examples

This file is NOT collected by the default test run.
"""

import io

import pytest

import rawobjstr

fsspec = pytest.importorskip("fsspec")
pa = pytest.importorskip("pyarrow")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

STORE_SIZE = 100 * 1024 * 1024  # 100 MB


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def store_path(tmp_path):
    """Format a fresh 100 MB store and return its path."""
    path = str(tmp_path / "pyarrow.raw")
    store = rawobjstr.format(path, size=STORE_SIZE)
    store.flush_index()
    del store
    return path


@pytest.fixture
def fs(store_path):
    """Open the store through fsspec."""
    return fsspec.filesystem("rawobjstr", path=store_path)


@pytest.fixture
def sample_table():
    """A small Arrow table -- sensor readings."""
    return pa.table({
        "sensor_id": pa.array(["S1", "S2", "S3", "S1", "S2"]),
        "timestamp": pa.array([1000, 1000, 1000, 2000, 2000]),
        "value": pa.array([23.1, 45.7, 12.3, 24.0, 44.9]),
    })


# ---------------------------------------------------------------------------
# Examples
# ---------------------------------------------------------------------------

@pytest.mark.examples
class TestPyArrowParquet:
    """Save and load Parquet files using PyArrow's native Parquet API."""

    def test_write_parquet_then_read(self, fs, sample_table):
        pq = pytest.importorskip("pyarrow.parquet")

        # --- Write ---
        # Serialize table to Parquet in memory
        buf = io.BytesIO()
        pq.write_table(sample_table, buf)
        parquet_bytes = buf.getvalue()

        # Store in rawobjstr
        fs.pipe("test/readings.parquet", parquet_bytes)

        # --- Read ---
        raw = fs.cat("test/readings.parquet")
        table = pq.read_table(io.BytesIO(raw))

        assert table.num_rows == 5
        assert table.num_columns == 3
        assert table.column_names == ["sensor_id", "timestamp", "value"]

    def test_read_parquet_specific_columns(self, fs, sample_table):
        pq = pytest.importorskip("pyarrow.parquet")

        # Write full table
        buf = io.BytesIO()
        pq.write_table(sample_table, buf)
        fs.pipe("test/full.parquet", buf.getvalue())

        # Read only selected columns
        raw = fs.cat("test/full.parquet")
        table = pq.read_table(io.BytesIO(raw), columns=["sensor_id", "value"])

        assert table.num_columns == 2
        assert "timestamp" not in table.column_names


@pytest.mark.examples
class TestPyArrowCSV:
    """Save and load CSV files using PyArrow's CSV reader/writer."""

    def test_write_csv_then_read(self, fs, sample_table):
        csv = pytest.importorskip("pyarrow.csv")

        # --- Write ---
        buf = io.BytesIO()
        csv.write_csv(sample_table, buf)
        csv_bytes = buf.getvalue()

        fs.pipe("test/readings.csv", csv_bytes)

        # --- Read ---
        raw = fs.cat("test/readings.csv")
        table = csv.read_csv(io.BytesIO(raw))

        assert table.num_rows == 5
        assert "sensor_id" in table.column_names

    def test_csv_with_read_options(self, fs, sample_table):
        csv = pytest.importorskip("pyarrow.csv")

        # Write
        buf = io.BytesIO()
        csv.write_csv(sample_table, buf)
        fs.pipe("test/typed.csv", buf.getvalue())

        # Read with explicit column types
        raw = fs.cat("test/typed.csv")
        convert_options = csv.ConvertOptions(
            column_types={
                "sensor_id": pa.string(),
                "timestamp": pa.int64(),
                "value": pa.float64(),
            }
        )
        table = csv.read_csv(io.BytesIO(raw), convert_options=convert_options)

        assert table.schema.field("value").type == pa.float64()
        assert table.schema.field("timestamp").type == pa.int64()


@pytest.mark.examples
class TestPyArrowIPC:
    """Save and load Arrow IPC (Feather) format -- zero-copy friendly."""

    def test_ipc_roundtrip(self, fs, sample_table):
        ipc = pytest.importorskip("pyarrow.ipc")

        # --- Write ---
        buf = io.BytesIO()
        writer = ipc.new_stream(buf, sample_table.schema)
        writer.write_table(sample_table)
        writer.close()
        ipc_bytes = buf.getvalue()

        fs.pipe("test/readings.arrow", ipc_bytes)

        # --- Read ---
        raw = fs.cat("test/readings.arrow")
        reader = ipc.open_stream(io.BytesIO(raw))
        table = reader.read_all()

        assert table.num_rows == 5
        assert table.equals(sample_table)


@pytest.mark.examples
class TestPyArrowBatchProcessing:
    """Demonstrate storing multiple record batches."""

    def test_store_multiple_batches(self, fs):
        # Create two separate batches (e.g. from streaming data)
        batch1 = pa.record_batch({
            "id": pa.array([1, 2, 3]),
            "label": pa.array(["a", "b", "c"]),
        })
        batch2 = pa.record_batch({
            "id": pa.array([4, 5, 6]),
            "label": pa.array(["d", "e", "f"]),
        })

        # Store each as a separate Parquet file
        pq = pytest.importorskip("pyarrow.parquet")

        for i, batch in enumerate([batch1, batch2]):
            table = pa.Table.from_batches([batch])
            buf = io.BytesIO()
            pq.write_table(table, buf)
            fs.pipe(f"batches/part_{i}.parquet", buf.getvalue())

        # List stored parts
        parts = fs.ls("batches", detail=False)
        assert len(parts) == 2

        # Read and combine
        tables = []
        for name in sorted(parts):
            raw = fs.cat(name)
            tables.append(pq.read_table(io.BytesIO(raw)))
        combined = pa.concat_tables(tables)
        assert combined.num_rows == 6
