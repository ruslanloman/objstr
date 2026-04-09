"""DuckDB + rawobjstr integration tests.

Tests the examples from the README and additional DuckDB workflows:
  - Arrow IPC round-trip (DuckDB -> rawobjstr -> DuckDB)
  - Parquet round-trip (DuckDB -> rawobjstr -> DuckDB)
  - Multiple tables in a single store
  - Byte-range reads for Parquet metadata
  - fsspec integration with DuckDB
"""

import io
import os
import tempfile

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import rawobjstr


STORE_SIZE = 128 * 1024 * 1024  # 128 MB


@pytest.fixture
def store(tmp_path):
    path = str(tmp_path / "duckdb_test.raw")
    s = rawobjstr.format(path, size=STORE_SIZE)
    yield s
    s.flush_index()


class TestArrowIPC:
    """Arrow IPC round-trip: DuckDB -> Arrow -> rawobjstr -> Arrow -> DuckDB."""

    def test_readme_example(self, store):
        """Exact example from the Python README."""
        con = duckdb.connect()
        table = con.sql(
            "SELECT i AS id, random() AS val FROM range(1_000_000) t(i)"
        ).to_arrow_table()

        # Write
        sink = pa.BufferOutputStream()
        writer = pa.ipc.new_file(sink, table.schema)
        writer.write_table(table)
        writer.close()
        store.put("analytics/result.arrow", sink.getvalue().to_pybytes())
        store.flush_index()

        # Read back
        raw = store.get("analytics/result.arrow")
        reader = pa.ipc.open_file(pa.BufferReader(raw))
        table2 = reader.read_all()

        result = duckdb.sql("SELECT count(*) AS cnt, avg(val) AS avg_val FROM table2")
        row = result.fetchone()
        assert row[0] == 1_000_000
        assert 0.0 < row[1] < 1.0  # avg of random() should be ~0.5

    def test_schema_preserved(self, store):
        """Schema types survive the round-trip."""
        con = duckdb.connect()
        table = con.sql("""
            SELECT
                42::INTEGER AS int_col,
                3.14::DOUBLE AS float_col,
                'hello'::VARCHAR AS str_col,
                TRUE AS bool_col
        """).to_arrow_table()

        sink = pa.BufferOutputStream()
        writer = pa.ipc.new_file(sink, table.schema)
        writer.write_table(table)
        writer.close()
        store.put("schema_test.arrow", sink.getvalue().to_pybytes())

        raw = store.get("schema_test.arrow")
        reader = pa.ipc.open_file(pa.BufferReader(raw))
        table2 = reader.read_all()

        assert table2.schema == table.schema
        assert table2.to_pydict() == table.to_pydict()

    def test_empty_table(self, store):
        """Empty table round-trip."""
        con = duckdb.connect()
        table = con.sql(
            "SELECT i AS id FROM range(0) t(i)"
        ).to_arrow_table()

        sink = pa.BufferOutputStream()
        writer = pa.ipc.new_file(sink, table.schema)
        writer.write_table(table)
        writer.close()
        store.put("empty.arrow", sink.getvalue().to_pybytes())

        raw = store.get("empty.arrow")
        reader = pa.ipc.open_file(pa.BufferReader(raw))
        table2 = reader.read_all()
        assert table2.num_rows == 0


class TestParquet:
    """Parquet round-trip: DuckDB -> Parquet -> rawobjstr -> Parquet -> DuckDB."""

    def test_parquet_round_trip(self, store):
        """Write Parquet from DuckDB, read back via DuckDB."""
        con = duckdb.connect()
        table = con.sql(
            "SELECT i AS id, i * 2 AS doubled FROM range(10_000) t(i)"
        ).to_arrow_table()

        # Write as Parquet
        buf = io.BytesIO()
        pq.write_table(table, buf)
        store.put("data.parquet", buf.getvalue())
        store.flush_index()

        # Read back
        raw = store.get("data.parquet")
        table2 = pq.read_table(io.BytesIO(raw))

        result = duckdb.sql(
            "SELECT count(*) AS cnt, sum(doubled) AS s FROM table2"
        ).fetchone()
        assert result[0] == 10_000
        assert result[1] == sum(i * 2 for i in range(10_000))

    def test_parquet_column_types(self, store):
        """Various column types survive Parquet round-trip."""
        con = duckdb.connect()
        table = con.sql("""
            SELECT
                i AS id,
                i::DOUBLE AS float_val,
                printf('row_%05d', i) AS label,
                i % 2 = 0 AS is_even
            FROM range(1000) t(i)
        """).to_arrow_table()

        buf = io.BytesIO()
        pq.write_table(table, buf)
        store.put("typed.parquet", buf.getvalue())

        raw = store.get("typed.parquet")
        table2 = pq.read_table(io.BytesIO(raw))

        # Verify a few rows via DuckDB
        result = duckdb.sql("""
            SELECT id, float_val, label, is_even
            FROM table2
            WHERE id IN (0, 1, 999)
            ORDER BY id
        """).fetchall()
        assert result[0] == (0, 0.0, "row_00000", True)
        assert result[1] == (1, 1.0, "row_00001", False)
        assert result[2] == (999, 999.0, "row_00999", False)

    def test_parquet_metadata_readable(self, store):
        """Parquet metadata/footer is accessible via range reads."""
        con = duckdb.connect()
        table = con.sql("SELECT i AS id FROM range(100) t(i)").to_arrow_table()
        buf = io.BytesIO()
        pq.write_table(table, buf)
        data = buf.getvalue()
        store.put("meta_test.parquet", data)

        # Read last 8 bytes (PAR1 magic + footer length)
        meta = store.head("meta_test.parquet")
        tail = store.get("meta_test.parquet", range=(meta.size - 8, meta.size))
        assert len(tail) == 8
        assert tail[-4:] == b"PAR1"

        # Read first 4 bytes (PAR1 magic)
        header = store.get("meta_test.parquet", range=(0, 4))
        assert header == b"PAR1"


class TestMultipleTables:
    """Multiple tables stored and queried independently."""

    def test_multiple_tables(self, store):
        con = duckdb.connect()

        # Store two different tables
        t1 = con.sql("SELECT i AS id, i * 10 AS val FROM range(500) t(i)").to_arrow_table()
        t2 = con.sql(
            "SELECT i AS key, printf('v%d', i) AS label FROM range(200) t(i)"
        ).to_arrow_table()

        for name, table in [("t1.parquet", t1), ("t2.parquet", t2)]:
            buf = io.BytesIO()
            pq.write_table(table, buf)
            store.put(name, buf.getvalue())
        store.flush_index()

        # Read both back and query
        raw1 = store.get("t1.parquet")
        raw2 = store.get("t2.parquet")
        r1 = pq.read_table(io.BytesIO(raw1))
        r2 = pq.read_table(io.BytesIO(raw2))

        cnt1 = duckdb.sql("SELECT count(*) FROM r1").fetchone()[0]
        cnt2 = duckdb.sql("SELECT count(*) FROM r2").fetchone()[0]
        assert cnt1 == 500
        assert cnt2 == 200

    def test_overwrite_and_requery(self, store):
        """Overwrite a table and verify the new data is returned."""
        con = duckdb.connect()

        # Write v1
        t1 = con.sql("SELECT 1 AS version, i AS id FROM range(10) t(i)").to_arrow_table()
        buf = io.BytesIO()
        pq.write_table(t1, buf)
        store.put("versioned.parquet", buf.getvalue())
        store.flush_index()

        # Write v2 (overwrite)
        t2 = con.sql("SELECT 2 AS version, i AS id FROM range(20) t(i)").to_arrow_table()
        buf = io.BytesIO()
        pq.write_table(t2, buf)
        store.put("versioned.parquet", buf.getvalue())
        store.flush_index()

        # Should get v2
        raw = store.get("versioned.parquet")
        result_tbl = pq.read_table(io.BytesIO(raw))
        result = duckdb.sql(
            "SELECT version, count(*) AS cnt FROM result_tbl GROUP BY version"
        ).fetchone()
        assert result == (2, 20)


class TestFsspecDuckDB:
    """DuckDB via fsspec (if fsspec is available)."""

    def test_fsspec_read(self, store, tmp_path):
        """Write via rawobjstr, read via fsspec into DuckDB."""
        fsspec = pytest.importorskip("fsspec")
        from rawobjstr.fsspec_impl import RawObjStFileSystem

        con = duckdb.connect()
        table = con.sql("SELECT i AS id FROM range(50) t(i)").to_arrow_table()
        buf = io.BytesIO()
        pq.write_table(table, buf)
        store.put("fs/data.parquet", buf.getvalue())
        store.flush_index()

        # Read via fsspec
        fs = RawObjStFileSystem(store=store)
        with fs.open("fs/data.parquet", "rb") as f:
            table2 = pq.read_table(f)

        cnt = duckdb.sql("SELECT count(*) FROM table2").fetchone()[0]
        assert cnt == 50

    def test_fsspec_write_read(self, store):
        """Write via fsspec, read back via rawobjstr into DuckDB."""
        fsspec = pytest.importorskip("fsspec")
        from rawobjstr.fsspec_impl import RawObjStFileSystem

        con = duckdb.connect()
        table = con.sql("SELECT i AS x FROM range(25) t(i)").to_arrow_table()
        buf = io.BytesIO()
        pq.write_table(table, buf)

        fs = RawObjStFileSystem(store=store)
        fs.pipe("fswrite.parquet", buf.getvalue())

        # Read back via rawobjstr directly
        raw = store.get("fswrite.parquet")
        table2 = pq.read_table(io.BytesIO(raw))
        cnt = duckdb.sql("SELECT count(*) FROM table2").fetchone()[0]
        assert cnt == 25


class TestLargerDatasets:
    """Slightly larger datasets to stress the integration."""

    def test_1m_rows_parquet(self, store):
        """1M row Parquet round-trip."""
        con = duckdb.connect()
        table = con.sql("""
            SELECT
                i AS id,
                random() AS val,
                printf('category_%d', i % 100) AS cat
            FROM range(1_000_000) t(i)
        """).to_arrow_table()

        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        data = buf.getvalue()
        store.put("big.parquet", data)
        store.flush_index()

        raw = store.get("big.parquet")
        table2 = pq.read_table(io.BytesIO(raw))

        result = duckdb.sql("""
            SELECT count(*) AS cnt, count(DISTINCT cat) AS cats
            FROM table2
        """).fetchone()
        assert result[0] == 1_000_000
        assert result[1] == 100

    def test_aggregation_query(self, store):
        """Run a non-trivial aggregation query on stored data."""
        con = duckdb.connect()
        table = con.sql("""
            SELECT
                i % 10 AS group_id,
                i AS value
            FROM range(10_000) t(i)
        """).to_arrow_table()

        buf = io.BytesIO()
        pq.write_table(table, buf)
        store.put("agg.parquet", buf.getvalue())

        raw = store.get("agg.parquet")
        table2 = pq.read_table(io.BytesIO(raw))

        result = duckdb.sql("""
            SELECT group_id, count(*) AS cnt, sum(value) AS total
            FROM table2
            GROUP BY group_id
            ORDER BY group_id
        """).fetchall()
        assert len(result) == 10
        for row in result:
            assert row[1] == 1000  # each group has 1000 rows
