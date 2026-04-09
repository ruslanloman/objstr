# objstrd Lance Benchmarks

LanceDB write + scan benchmarks measuring the overhead of the S3 HTTP
protocol layer by connecting LanceDB to objstrd's S3 endpoint instead
of accessing the object store directly.

For each backend, the benchmark starts an `objstrd` process, connects
LanceDB via S3 `storage_options`, runs the workload, then kills the
process.

## Prerequisites

```bash
source ~/rawobjstr/.venv/bin/activate
pip install pylance pyarrow numpy

# Build objstrd (release)
cd ~/objstr
CARGO_TARGET_DIR=~/build-objstrd ~/.cargo/bin/cargo build \
  -p objstrd --release
```

## Usage (Python)

```bash
cd ~/objstr/objstrd/benchmarks/lance

# All 6 backends (requires root for /dev/sdb and drop_caches)
sudo ~/rawobjstr/.venv/bin/python bench_write_scan.py \
  --objstrd-bin ~/build-objstrd/release/objstrd --device /dev/sdb

# Quick test (in-memory, 5 chunks)
python3 bench_write_scan.py --backends mem --chunks 5 \
  --objstrd-bin ~/build-objstrd/release/objstrd

# Single backend
python3 bench_write_scan.py --backends fs \
  --objstrd-bin ~/build-objstrd/release/objstrd

# Image-only (no root needed for device)
python3 bench_write_scan.py --backends img+directio,img+buffered \
  --objstrd-bin ~/build-objstrd/release/objstrd
```

## Usage (Rust)

```bash
# Build (requires the lance feature)
cd ~/objstr
CARGO_TARGET_DIR=~/build-objstrd ~/.cargo/bin/cargo build \
  --example bench_lance -p objstrd --features lance --release

# All 6 backends (requires root for /dev/sdb and drop_caches)
sudo ~/build-objstrd/release/examples/bench_lance \
  --objstrd-bin ~/build-objstrd/release/objstrd \
  --device /dev/sdb --backends all

# Quick test
~/build-objstrd/release/examples/bench_lance \
  --objstrd-bin ~/build-objstrd/release/objstrd \
  --backends mem --chunks 5
```

## Backends

| Label | objstrd Backend | Storage |
|-------|-----------------|---------|
| fs | `--backend fs` | LocalFileSystem via S3 (equiv to "ext4" in other benches) |
| mem | `--backend mem` | In-memory, isolates pure HTTP overhead |
| img+directio | `--image <path> --direct-io` | RawObjectStore, 16 GB image, O_DIRECT via S3 |
| img+buffered | `--image <path>` | RawObjectStore, 16 GB image, buffered via S3 |
| raw+directio | `--image /dev/sdb --direct-io` | RawObjectStore, raw block device via S3 |
| raw+buffered | `--image /dev/sdb` | RawObjectStore, raw block device via S3 |

## Comparing with rawobjstr / shardedobjstr

Run the rawobjstr and shardedobjstr benchmarks first, then these. The
results show how much overhead the S3 HTTP layer (s3s, SigV4, hyper,
TCP loopback) adds on top of the sharded object store.
