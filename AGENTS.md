# AGENTS.md - Workspace-wide Hints for AI Agents & Contributors

This file contains hints that apply to every crate in this workspace.
Each sub-crate has its own `AGENTS.md` with crate-specific details.

---

## Workspace Layout

| Crate | Path | Purpose |
|-------|------|---------|
| `rawobjstr` | `rawobjstr/` | Core library: writes directly to raw block devices, implements `ObjectStore` |
| `objstrd` | `objstrd/` | S3-compatible distributed object store daemon (standalone, node, coordinator) |
| `shardedobjstr` | `shardedobjstr/` | Distributes objects across multiple shards with replication |
| `objstrproxyd` | `objstrproxyd/` | **(planned, not yet a crate)** Stateless edge proxy: striped range reads across replicas for high-throughput GETs |
| `pyrawobjstr` | `rawobjstr/python/` | Python bindings for `rawobjstr` (pip: `rawobjstr`) |
| `pyshardedobjstr` | `shardedobjstr/python/` | Python bindings for the sharded crate (pip: `shardedobjstr`) |

All crates live in a single Cargo workspace (`Cargo.toml` at the repo root).

---

## Coding Rules

- **NEVER commit, add, or merge to git without explicit approval from the user.** Always ask before running `git add`, `git commit`, `git merge`, `git push`, or any command that modifies git history or the staging area.
- **NEVER use non-ASCII characters in code or shell scripts.** No em dashes, curly quotes, ellipsis characters, multiplication signs, arrows, checkmarks, etc. Use only plain ASCII equivalents.
- **NEVER use em dashes in documentation.**
- **NEVER run `cargo check`, `cargo build`, or `cargo test` on Windows.** The Windows toolchain is unreliable for this workspace (PATH issues, MAX_PATH limits, missing Unix headers). ALL compilation and testing MUST happen on the Linux VM via SSH. Edit files locally on Windows, `scp` them to the VM, then run cargo commands there.
- These crates require Linux for full functionality (O_DIRECT, `BLKGETSIZE64` ioctl). They can compile on other platforms but some block device options are Linux-only.
- **O_DIRECT:** On a raw block device, O_DIRECT is enabled automatically. All reads and writes must be 4 KB aligned. The store handles alignment internally -- the caller does not need to worry about this.
- PowerShell does not support heredoc. Write scripts to a file and scp them instead.

---

## Adding New Features

When adding new features:

- **Keep the Python library in sync** if the crate has one (`pyrawobjstr` or `pyshardedobjstr`).
- **Add new tests and e2e tests** for the Rust crate and also add or update the Python library tests.
- **Update documentation** related to the feature -- for CLI tools give usage examples, for the Python library add code snippets.
- **Check both write paths in `shardedobjstr`**: normal puts go through `lib.rs` (`write_to_shards_inner`), metadata-aware puts go through `metadata.rs` (`put_with_meta`, `put_with_meta_from_file`). They have separate replication loops. If you change replication, quorum, or write-error logic, update both. See `shardedobjstr/AGENTS.md` for details.

---

## Testing Philosophy: Compose Building Blocks

When writing tests, always use the existing building blocks and compose them
together. Any `ObjectStore` backend can be presented as an S3 endpoint by
putting an `objstrd` (or in-process `TestServer`) on top of it. This means:

- To test against a raw store over S3: spin up `TestServer` with a
  `RawObjectStore` underneath, then talk to it via HTTP with `reqwest`.
- To test an S3 shard in a `ShardedObjectStore`: put a `TestServer` on a raw
  image so it speaks S3, then point the sharded store at that endpoint.
- To test any `ObjectStore` trait backend: wrap it in `ObjectStoreS3Adapter`
  and serve it -- even if the backend is itself another S3 client.

**Do NOT add new Cargo dependencies** (e.g. `object_store` with the `aws`
feature in dev-deps) just to construct a client in test code.  Instead stack
our own servers: `RawObjectStore` -> `TestServer` -> HTTP/S3 endpoint.
This keeps tests closer to real deployments and avoids pulling in extra
crates.

---

## Code Placement: Library vs Daemon

**Do NOT put reusable logic in `objstrd` that belongs in `shardedobjstr`.**

`objstrd` is a daemon -- it should only contain HTTP/S3 protocol handling,
background task loops, daemon lifecycle, and config parsing. Any algorithm or
data operation that CLI tools (`shardedobjstr`) or Python bindings
(`shardedobjstr`) would also need **must** live in the `shardedobjstr`
library crate. `objstrd` then calls into the library.

Examples of what belongs in the **library** (`shardedobjstr`):
- Replication repair / repair-replication sweeps
- Over-replication trimming
- Health probing (`probe_store`)
- Mirror sync / partitioned sync algorithms
- Metadata TLV encoding/decoding
- Metadata-aware I/O routing (`put_with_meta`, `head_with_meta`, etc.)

Examples of what belongs in the **daemon** (`objstrd`):
- S3 protocol adapter (`ObjectStoreS3Adapter`, multipart upload state)
- Background polling loops (`recovery_loop`, `spawn_recovery_task`)
- Web UI status endpoints (`/_admin/recovery`, `/_admin/info`)
- Config file parsing, CLI flags, logging setup

---

## Development VM

All building, testing, and benchmarking happens on a Linux VM:

| Setting | Value |
|---------|-------|
| Host |  **this is DHCP -- ask user for ip** |
| User | **ask user what username to use** |
| Sudo password | **ask user at start of session and remember it** |
| Cargo env | `source ~/.cargo/env` (required in every new SSH session) |
| Cargo bin | `~/.cargo/bin/cargo` -- use directly to avoid `source` issues in non-login SSH |

**Tip:** Use `~/.cargo/bin/cargo` directly rather than `source ~/.cargo/env`,
which often fails in non-login SSH sessions.

**Tip:** Use `-o ControlPath=none` to bypass stale SSH multiplexing if connections hang.

### Deploying Code to the VM

From the Windows machine, use `scp` to copy changed files:

```powershell
# Single file example
scp rawobjstr\src\store.rs user@virtualmachineip:~/objstr/rawobjstr/src/store.rs

# Entire crate example
scp -r rawobjstr\ user@virtualmachineip:~/objstr/rawobjstr/
```

**If you hit a problem trying to build and keep getting stuck ask user to help they can install software or help debug**

Update information on building if new issues are found which are not documented in the AGENTS.md files.

### Per-crate Paths on the VM

| Crate | Source on VM | CARGO_TARGET_DIR | Release binary |
|-------|-------------|------------------|----------------|
| `rawobjstr` | `~/objstr/rawobjstr` | `~/build-rawobjstr` | -- (library) |
| `objstrd` | `~/objstr/objstrd` | `~/build-objstrd` | `~/build-objstrd/release/objstrd` |
| `shardedobjstr` | `~/objstr/shardedobjstr` | `~/build-sharded` | `shardedobjstr`, `shardedobjstr-catview` |
| `pyrawobjstr` | `~/objstr/rawobjstr/python` | `~/build-pyrawobjstr` | -- (Python wheel) |
| `pyshardedobjstr` | `~/objstr/shardedobjstr/python` | `~/build-pyshardedobjstr` | -- (Python wheel) |

**Building any crate** (always on the VM):

```bash
cd ~/objstr
CARGO_TARGET_DIR=~/build-<name> ~/.cargo/bin/cargo build -p <crate> --release
```

**Deploying any crate** (from Windows):

```powershell
# Single file
scp <crate>\src\file.rs user@virtualmachineip:~/objstr/<crate>/src/file.rs

# Entire crate
scp -r <crate>\ user@virtualmachineip:~/objstr/<crate>/
```

**Running tests** for any crate:

```bash
CARGO_TARGET_DIR=~/build-<name> ~/.cargo/bin/cargo test -p <crate> --release
```

See each crate's `AGENTS.md` for crate-specific test examples and architecture details.

### Python Environment on the VM

The Python bindings (`pyrawobjstr`, `pyshardedobjstr`) are built with `maturin`
inside a virtualenv. After a VM wipe, recreate the environment:

```bash
# Prerequisites (need sudo)
sudo apt-get install -y build-essential python3-dev python3-venv

# Create virtualenv at the workspace root
python3 -m venv ~/objstr/.venv

# Activate and install tools
source ~/objstr/.venv/bin/activate
pip install pytest maturin
```

Building a Python crate (example for `pyrawobjstr`):

```bash
export PATH=~/.cargo/bin:$PATH
source ~/objstr/.venv/bin/activate
cd ~/objstr/rawobjstr/python
CARGO_TARGET_DIR=~/build-pyrawobjstr maturin develop --release
```

**Important:** Both `cargo` in PATH and an active virtualenv are required for
`maturin develop`. Without the venv, maturin refuses to run. Without `cc` in
PATH (`build-essential`), the Rust compiler cannot link.

---

## Windows Machine Notes

**Windows is for editing files ONLY. Do NOT run cargo on Windows.**

Use the workflow: edit locally -> `scp` to VM -> run cargo on VM.

- **Cargo/rustc:** `C:\msys64\ucrt64\bin\cargo.exe` (MSYS2 UCRT64, NOT rustup) -- DO NOT USE for this workspace
- **rustup is NOT installed** -- the toolchain comes from MSYS2 pacman packages
- **Cannot build or test on Windows** -- O_DIRECT and block device ioctl are Linux-only, PATH and MAX_PATH issues are common, and builds frequently fail or hang. Always use the Linux VM instead.
- **CARGO_TARGET_DIR:** Deeply nested builds can exceed MAX_PATH (260 chars). This is another reason to use the VM.

### PowerShell Quote Mangling

**NEVER pass complex shell commands inline via `ssh ... "command"` from
PowerShell.** PowerShell aggressively re-interprets quotes, backslashes,
dollar signs, and special characters inside double-quoted strings before
they reach SSH. This causes silent corruption -- strings get truncated,
variables expand to empty, and escape sequences break.

Symptoms: `SyntaxError: unterminated string literal`, missing property
name errors, `IncorrectValueForFormatParameter`, or commands that simply
produce wrong output.

**Instead, write the command to a local `.sh` file, `scp` it to the VM,
and execute it there:**

```powershell
# WRONG -- PowerShell will mangle the quotes and $variables:
ssh user@virtualmachineip "python -c 'import sys; print(sys.path)'"

# RIGHT -- write to a file, scp, execute:
#   1. Create the script locally (or use create_file)
#   2. scp script.sh user@virtualmachineip:/tmp/script.sh
#   3. ssh user@virtualmachineip "bash /tmp/script.sh"
```

Simple commands with no quotes, no `$`, and no special chars are fine
inline (e.g. `ssh user@virtualmachineip "ls -la /tmp"`).  When in doubt,
use the scp approach.

---

## Build Version Baking

Every crate's `build.rs` bakes compile-time version info into the binary at build time:

| Env var | Source | Example |
|---------|--------|---------|
| `BUILD_GIT_HASH` | `git rev-parse --short HEAD` (appends `-dirty` if modified) | `a1b2c3d-dirty` |
| `BUILD_DATE` | `date -u '+%Y-%m-%d %H:%M:%S UTC'` | `2026-03-26 10:00:00 UTC` |

Exposed as:
- **CLI (`rawobjstr`):** `rawobjstr --version`
- **CLI (`shardedobjstr`):** `shardedobjstr version`
- **Daemon (`objstrd`):** `GET /_admin/info` returns `build_git_hash` and `build_date`
- **Python (`rawobjstr`):** `rawobjstr.__version__`, `rawobjstr.__build_info__`
- **Python (`shardedobjstr`):** `shardedobjstr.__version__`, `shardedobjstr.__build_info__`

---

## Binary Swap Detection (Test Hygiene)

When a `cargo build` runs in the background while a test suite is in progress,
the binary on disk is silently replaced. Tests that fail may be caused by a
server crash, restart, or half-restart -- not an actual bug.

**Pattern for any external test suite that spawns `objstrd` as a subprocess
(ceph s3-tests, rclone, mint):**

```python
import requests

def get_server_build(base_url: str) -> str | None:
    """Return build_git_hash from /_admin/info, or None if unreachable."""
    try:
        r = requests.get(f"{base_url}/_admin/info", timeout=3)
        return r.json().get("build_git_hash") if r.ok else None
    except Exception:
        return None

def check_server_still_valid(base_url: str, expected_hash: str) -> str:
    """Return 'ok', 'crashed', or 'replaced'."""
    actual = get_server_build(base_url)
    if actual is None:
        return "crashed"
    return "ok" if actual == expected_hash else "replaced"

# In test setup / conftest:
expected_hash = get_server_build(BASE_URL)

# In teardown / finally (only when failures > 0):
state = check_server_still_valid(BASE_URL, expected_hash)
if state == "crashed":
    print("WARNING: server not responding -- failures may be caused by a crash")
elif state == "replaced":
    print(f"WARNING: binary replaced during testing (expected {expected_hash})")
    print("All results may be invalid. Rerun when no cargo build is running.")
```

1. Capture `build_git_hash` from `/_admin/info` before the suite runs.
2. Re-check in the cleanup/finally path if any test failed.
3. If `replaced` or `crashed`, print a WARNING banner (not a test failure).
