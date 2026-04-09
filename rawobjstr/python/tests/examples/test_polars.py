"""Polars + rawobjstr fsspec example.

Shows how to save and load DataFrames (CSV and Parquet) on a rawobjstr
store using Polars.

Requirements:
    pip install fsspec polars

Run:
    pytest tests/examples/test_polars.py -v -m examples

This file is NOT collected by the default test run.
"""

import io

import pytest

import rawobjstr

fsspec = pytest.importorskip("fsspec")
pl = pytest.importorskip("polars")

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
    path = str(tmp_path / "polars.raw")
    store = rawobjstr.format(path, size=STORE_SIZE)
    store.flush_index()
    del store
    return path


@pytest.fixture
def fs(store_path):
    """Open the store through fsspec."""
    return fsspec.filesystem("rawobjstr", path=store_path)


@pytest.fixture
def sample_df():
    """A small DataFrame -- product inventory."""
    return pl.DataFrame({
        "product": ["Widget", "Gadget", "Doohickey", "Thingamajig"],
        "price": [9.99, 24.50, 4.75, 15.00],
        "quantity": [100, 42, 500, 73],
    })


# ---------------------------------------------------------------------------
# Examples
# ---------------------------------------------------------------------------

@pytest.mark.examples
class TestPolarsCSV:
    """Save and load CSV files with Polars."""

    def test_write_csv_then_read(self, fs, sample_df):
        # --- Write ---
        # Polars write_csv returns a string; encode to bytes
        csv_bytes = sample_df.write_csv().encode("utf-8")
        fs.pipe("inventory/products.csv", csv_bytes)

        # --- Read ---
        raw = fs.cat("inventory/products.csv")
        df = pl.read_csv(io.BytesIO(raw))

        assert df.columns == ["product", "price", "quantity"]
        assert len(df) == 4
        assert df["product"][0] == "Widget"

    def test_read_csv_from_file_handle(self, fs, sample_df):
        csv_bytes = sample_df.write_csv().encode("utf-8")
        fs.pipe("inventory/items.csv", csv_bytes)

        # Read using an fsspec file handle
        with fs.open("inventory/items.csv", "rb") as f:
            df = pl.read_csv(f)

        assert len(df) == 4
        total_value = (df["price"] * df["quantity"]).sum()
        assert total_value > 0


@pytest.mark.examples
class TestPolarsParquet:
    """Save and load Parquet files with Polars."""

    def test_write_parquet_then_read(self, fs, sample_df):
        # Polars can write Parquet to a BytesIO buffer
        buf = io.BytesIO()
        sample_df.write_parquet(buf)
        parquet_bytes = buf.getvalue()

        fs.pipe("inventory/products.parquet", parquet_bytes)

        # Read it back
        raw = fs.cat("inventory/products.parquet")
        df = pl.read_parquet(io.BytesIO(raw))

        assert df.columns == ["product", "price", "quantity"]
        assert len(df) == 4
        assert df["quantity"].sum() == 715


@pytest.mark.examples
class TestPolarsLazyCSV:
    """Demonstrate lazy scanning with explicit collect."""

    def test_lazy_csv_roundtrip(self, fs, sample_df):
        csv_bytes = sample_df.write_csv().encode("utf-8")
        fs.pipe("lazy/data.csv", csv_bytes)

        # Read CSV into a buffer, then scan lazily
        raw = fs.cat("lazy/data.csv")
        df = (
            pl.read_csv(io.BytesIO(raw))
            .lazy()
            .filter(pl.col("price") > 10)
            .select(["product", "price"])
            .collect()
        )

        assert len(df) == 2  # Gadget (24.50) and Thingamajig (15.00)
        assert set(df["product"].to_list()) == {"Gadget", "Thingamajig"}
