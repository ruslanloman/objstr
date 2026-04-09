"""Pandas + rawobjstr fsspec example.

Shows how to save and load DataFrames (CSV and Parquet) on a rawobjstr
store using pandas' built-in fsspec integration.

Requirements:
    pip install fsspec pandas pyarrow   (pyarrow needed for Parquet)

Run:
    pytest tests/examples/test_pandas.py -v -m examples

This file is NOT collected by the default test run.
"""

import io

import pytest

import rawobjstr

fsspec = pytest.importorskip("fsspec")
pd = pytest.importorskip("pandas")

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
    path = str(tmp_path / "pandas.raw")
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
    """A small DataFrame for testing -- city temperature readings."""
    return pd.DataFrame({
        "city": ["London", "Paris", "Tokyo", "New York", "Sydney"],
        "temp_c": [15.2, 18.5, 22.1, 12.8, 25.3],
        "humidity": [78, 65, 55, 82, 45],
    })


# ---------------------------------------------------------------------------
# Examples
# ---------------------------------------------------------------------------

@pytest.mark.examples
class TestPandasCSV:
    """Save and load CSV files on a rawobjstr store with pandas."""

    def test_write_csv_then_read(self, fs, sample_df):
        # --- Write a DataFrame as CSV into the store ---
        # Serialize to bytes, then pipe into the store
        csv_bytes = sample_df.to_csv(index=False).encode("utf-8")
        fs.pipe("weather/cities.csv", csv_bytes)

        # --- Read the CSV back into a DataFrame ---
        raw = fs.cat("weather/cities.csv")
        df = pd.read_csv(io.BytesIO(raw))

        assert list(df.columns) == ["city", "temp_c", "humidity"]
        assert len(df) == 5
        assert df.loc[0, "city"] == "London"

    def test_open_file_handle_for_csv(self, fs, sample_df):
        # Write CSV using a file handle (like writing to a local file)
        csv_text = sample_df.to_csv(index=False)
        with fs.open("weather/report.csv", "wb") as f:
            f.write(csv_text.encode("utf-8"))

        # Read CSV using a file handle
        with fs.open("weather/report.csv", "rb") as f:
            df = pd.read_csv(f)

        assert len(df) == 5
        assert df["temp_c"].mean() == pytest.approx(18.78, abs=0.01)

    def test_multiple_csvs_in_directory(self, fs):
        # Write several CSV files under a prefix
        for i in range(3):
            df = pd.DataFrame({"x": range(i * 10, (i + 1) * 10)})
            csv_bytes = df.to_csv(index=False).encode("utf-8")
            fs.pipe(f"batches/batch_{i}.csv", csv_bytes)

        # List them
        files = fs.ls("batches", detail=False)
        assert len(files) == 3

        # Read and concatenate
        frames = []
        for name in sorted(files):
            raw = fs.cat(name)
            frames.append(pd.read_csv(io.BytesIO(raw)))
        combined = pd.concat(frames, ignore_index=True)
        assert len(combined) == 30


@pytest.mark.examples
class TestPandasParquet:
    """Save and load Parquet files -- requires pyarrow."""

    def test_write_parquet_then_read(self, fs, sample_df):
        pa = pytest.importorskip("pyarrow")

        # Serialize DataFrame to Parquet bytes
        buf = io.BytesIO()
        sample_df.to_parquet(buf, index=False)
        parquet_bytes = buf.getvalue()

        # Store it
        fs.pipe("weather/cities.parquet", parquet_bytes)

        # Read it back
        raw = fs.cat("weather/cities.parquet")
        df = pd.read_parquet(io.BytesIO(raw))

        assert list(df.columns) == ["city", "temp_c", "humidity"]
        assert len(df) == 5
        assert df.loc[2, "city"] == "Tokyo"
