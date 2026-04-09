# Building rawobjstr

## Prerequisites

- **Rust 1.70+** with Cargo
- **C linker** (`cc` or `gcc`) - install via `build-essential` on Debian/Ubuntu
- **Linux** for full functionality (O_DIRECT, `BLKGETSIZE64` ioctl). Loopback files work on other platforms.
- **Python 3 + maturin** (only for Python bindings)

```bash
# Debian / Ubuntu
sudo apt-get install -y build-essential

# For Python bindings
sudo apt-get install -y python3-dev python3-venv
```

---

## Quick Build

The included `build.sh` script checks prerequisites and builds the binary:

```bash
./build.sh --location ~/build-raw
```

| Flag | Description |
|------|-------------|
| `--location <dir>` | Directory for build output (required, created if needed) |
| `--release` | Build in release mode (default) |
| `--debug` | Build in debug mode (faster compile, slower binary) |

---

## Manual Build

```bash
CARGO_TARGET_DIR=~/build-rawobjstr cargo build -p rawobjstr --release
```

Or using the full path to cargo (useful in non-login SSH sessions):

```bash
CARGO_TARGET_DIR=~/build-rawobjstr ~/.cargo/bin/cargo build -p rawobjstr --release
```

---

## Build Output

| Binary | Location |
|--------|----------|
| `rawobjstr` | `~/build-rawobjstr/release/rawobjstr` |

---

## Python Bindings

The Python bindings (`pyrawobjstr`) are built with maturin inside a virtualenv.

```bash
# One-time setup
python3 -m venv ~/objstr/.venv
source ~/objstr/.venv/bin/activate
pip install pytest maturin

# Build and install into the venv
export PATH=~/.cargo/bin:$PATH
source ~/objstr/.venv/bin/activate
cd rawobjstr/python
CARGO_TARGET_DIR=~/build-pyrawobjstr maturin develop --release
```

To build a wheel for distribution:

```bash
CARGO_TARGET_DIR=~/build-pyrawobjstr maturin build --release
```

The wheel lands in `~/build-pyrawobjstr/wheels/`.

---

## Build Version Info

`build.rs` bakes compile-time version info into the binary:

| Env Var | Source | Example |
|---------|--------|---------|
| `BUILD_GIT_HASH` | `git rev-parse --short HEAD` (appends `-dirty` if modified) | `a1b2c3d-dirty` |
| `BUILD_DATE` | `date -u '+%Y-%m-%d %H:%M:%S UTC'` | `2026-03-26 10:00:00 UTC` |

Check it at runtime:

```bash
rawobjstr --version
# rawobjstr 0.1.0 (git a1b2c3d, built 2026-03-26 10:00:00 UTC)
```

Python:

```python
import rawobjstr
print(rawobjstr.__version__)
print(rawobjstr.__build_info__)
```

---

## Running Tests

```bash
# All tests
CARGO_TARGET_DIR=~/build-rawobjstr ~/.cargo/bin/cargo test -p rawobjstr --release

# Single test file
CARGO_TARGET_DIR=~/build-rawobjstr ~/.cargo/bin/cargo test -p rawobjstr --release --test cli_tools

# Single test
CARGO_TARGET_DIR=~/build-rawobjstr ~/.cargo/bin/cargo test -p rawobjstr --release --test cli_tools cli_put_list_get_delete_export_roundtrip

# Python tests
source ~/objstr/.venv/bin/activate
cd rawobjstr/python
pytest tests/
```

See [TESTS.md](TESTS.md) for the full test inventory.

---

## Cleanup

```bash
rm -rf ~/build-rawobjstr
rm -rf ~/build-pyrawobjstr
```
