# Multi-Backend Demo

Starts an `objstrd` node with **5 different storage backends** behind a
single S3-compatible endpoint. Objects are replicated (RF=4) across the
backends via jump-consistent hashing.

## Backends

| Shard | Type | Description |
|-------|------|-------------|
| 0 | raw | Block device (`/dev/sdb`) |
| 1 | raw | Image file (`/tmp/demo.img`, zstd compressed) |
| 2 | fs | Filesystem directory (`/tmp/demo_fs`) |
| 3 | mem | In-memory (ephemeral) |
| 4 | s3 | S3 proxy to a second `objstrd` on port 8001 |

## Prerequisites

- `objstrd` binary built on the VM:
  ```
  cd ~/objstr
  CARGO_TARGET_DIR=~/build-objstrd ~/.cargo/bin/cargo build -p objstrd --release
  ```
- A block device at `/dev/sdb` (or edit `demo_cluster.conf` to remove it)
- Root access (needed for raw block device)

## Quick Start

From the repo root on the VM:

```bash
sudo bash demo/launch_demo.sh
```

Or from Windows, copy and run remotely:

```powershell
scp -o ControlPath=none -r demo\ test@vmserver:/tmp/demo/
ssh -o ControlPath=none test@vmserver "echo PASSWORD | sudo -S nohup bash /tmp/demo/launch_demo.sh > /tmp/demo.log 2>&1 & sleep 6; cat /tmp/demo.log"
```

## Web UIs

Once running:

- **Main UI** -- http://vmserver:8000/ui.html (browse objects, upload/download)
- **Server status** -- http://vmserver:8000/server.html (shard health, recovery)
- **Visualizer** -- http://vmserver:8000/viz.html (object placement map)
- **Backend S3 UI** -- http://vmserver:8001/ui.html (shard 4's backing store)

## Checking Status

Quick overview of recovery status, shard health, and object placement:

```bash
bash demo/check_quick.sh
```

Detailed state dump with automatic repair-replication:

```bash
bash demo/check_state.sh
```

## Draining a Shard

Demonstrates draining the in-memory shard (shard 3) and verifying that all
objects remain accessible:

```bash
bash demo/test_drain_mem.sh
```

This shows the before/after state: shard file counts, object placement, and
per-object GET verification.

## Hash Placement Tool

Shows which shards each key maps to under jump-consistent hashing:

```bash
python3 demo/check_hash.py
```

## Stopping

Kill both `objstrd` processes:

```bash
sudo pkill -f objstrd
```

## Files

| File | Purpose |
|------|---------|
| `launch_demo.sh` | Start the demo (kills old instances first) |
| `demo_cluster.conf` | Cluster config defining the 5 shards |
| `check_quick.sh` | Quick status: recovery, shards, objects |
| `check_state.sh` | Detailed state + repair-replication |
| `test_drain_mem.sh` | Drain workflow demo |
| `check_hash.py` | Jump-consistent-hash placement calculator |

## Configuration

Edit `demo_cluster.conf` to modify backends. Key settings:

- `rf=4` -- replication factor (number of shards each object is stored on)
- `compression=zstd` -- shard 1 uses zstd compression
