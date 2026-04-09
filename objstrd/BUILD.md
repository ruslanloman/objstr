# Building objstrd

## Prerequisites

- **Rust 1.70+** with Cargo
- **C linker** (`cc` or `gcc`) -- install via `build-essential` on Debian/Ubuntu
- **Linux** for full functionality (O_DIRECT, `BLKGETSIZE64` ioctl)

```bash
# Debian / Ubuntu
sudo apt-get install -y build-essential
```

---

## Quick Build

```bash
CARGO_TARGET_DIR=~/build-objstrd cargo build -p objstrd --release
```

Or using the full path to cargo (useful in non-login SSH sessions):

```bash
CARGO_TARGET_DIR=~/build-objstrd ~/.cargo/bin/cargo build -p objstrd --release
```

---

## Build Output

| Binary | Location |
|--------|----------|
| `objstrd` | `~/build-objstrd/release/objstrd` |

---

## Build Version Info

`build.rs` bakes compile-time version info into the binary:

| Env Var | Source | Example |
|---------|--------|---------|
| `BUILD_GIT_HASH` | `git rev-parse --short HEAD` (appends `-dirty` if modified) | `a1b2c3d-dirty` |
| `BUILD_DATE` | `date -u '+%Y-%m-%d %H:%M:%S UTC'` | `2026-03-26 10:00:00 UTC` |

Check it at runtime:

```bash
objstrd --version
# objstrd 0.1.0 (git a1b2c3d, built 2026-03-26 10:00:00 UTC)
```

Or via the admin API on a running server:

```bash
curl http://localhost:8000/_admin/info
# { "build_git_hash": "a1b2c3d", "build_date": "2026-03-26 10:00:00 UTC", ... }
```

---

## Running Tests

```bash
# All tests
CARGO_TARGET_DIR=~/build-objstrd ~/.cargo/bin/cargo test -p objstrd --release

# Single test file
CARGO_TARGET_DIR=~/build-objstrd ~/.cargo/bin/cargo test -p objstrd --release --test object_crud

# Single test
CARGO_TARGET_DIR=~/build-objstrd ~/.cargo/bin/cargo test -p objstrd --release --test object_crud test_put_then_get
```

See [TESTS.md](TESTS.md) for the full test inventory.

---

## Cleanup

```bash
rm -rf ~/build-objstrd
```
