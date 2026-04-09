# rclone Benchmark

Throughput benchmarks for `objstrd` using `rclone test speed`  -  rclone's
built-in S3 speed test. Seven phases covering small objects (4 Ki - 8 Mi),
large objects (64 Mi - 256 Mi), single PUT and multipart upload paths,
single-threaded and 4-thread concurrency, and a mixed concurrent workload.

Results are compared across all supported backends.

## Test Environment

| Parameter | Value |
|-----------|-------|
| VM | Hyper-V, 8 vCPU, 4 GB RAM, 4 GB swap |
| OS | Ubuntu 24.04 (linux 6.8) |
| Root disk | /dev/sda1 ext4, 97 GB (virtual SCSI) |
| objstrd | 0.1.0 (S3 daemon, s3s 0.13, hyper 1) |
| rawobjstr | 0.1.0 (raw object store engine) |
| rclone | v1.73.3 |

## Backends

| Backend | Description |
|---------|-------------|
| `mem` | `InMemory`  -  in-process memory store; theoretical ceiling for the HTTP + async stack |
| `fs` | `LocalFileSystem`  -  one file per object on the host filesystem |
| `raw-file` | `RawObjectStore` on a 2 GiB image file (no O_DIRECT) |
| `raw-file-dio` | `RawObjectStore` on a 2 GiB image file (O_DIRECT) |
| `raw-dev` | `RawObjectStore` on `/dev/sdb` block device (no O_DIRECT) |
| `raw-dev-dio` | `RawObjectStore` on `/dev/sdb` block device (O_DIRECT) |
| `raw-loop` | `RawObjectStore` on a loopback device backed by a file (no O_DIRECT) |
| `raw-loop-dio` | `RawObjectStore` on a loopback device backed by a file (O_DIRECT) |

## Phases

| Phase | Object sizes | Threads | Mode | Notes |
|-------|-------------|---------|------|-------|
| P1 | 4 Ki / 1 Mi / 8 Mi | 1 | single PUT | small, serial |
| P2 | 4 Ki / 1 Mi / 8 Mi | 4 | single PUT | small, concurrent |
| P3 | 64 Mi / 128 Mi / 256 Mi | 1 | single PUT | large, serial |
| P4 | 64 Mi / 128 Mi / 256 Mi | 4 | single PUT | large, concurrent |
| P5 | 64 Mi / 128 Mi / 256 Mi | 1 | multipart (16 Mi chunks) | large multipart, serial |
| P6 | 64 Mi / 128 Mi / 256 Mi | 4 | multipart (16 Mi chunks) | large multipart, concurrent |
| P7 | 4 Kix16t + 1 Mix8t + 256 Mix2t | mixed | mixed concurrent | flush pressure stress test |

P7 runs three rclone processes simultaneously for 30 s (quick) / 60 s (full).
All raw backends use `flush_interval = 2 s` so a 30 s mixed run triggers ~15
flushes.

## How to Run

**Prerequisite:** rclone v1.73+ (see
[rclone-tests.md](../../../external-tests/s3-compat/rclone/rclone-tests.md)
for install instructions  -  the `apt` package on Ubuntu 22.04 is broken).

```bash
# Deploy
scp objstrd/benchmarks/rclone/run_bench.sh test@vmserver:/tmp/rclone_tests/

# Quick run  -  subset of backends (~5 min)
ssh test@vmserver "setsid bash /tmp/rclone_tests/run_bench.sh --quick --backends mem,fs,raw-file </dev/null >/tmp/bench.log 2>&1 & echo pid=\$!"

# Full run  -  all backends (~2 hours)
ssh test@vmserver "setsid bash /tmp/rclone_tests/run_bench.sh </dev/null >/tmp/bench.log 2>&1 & echo pid=\$!"

# Monitor
ssh test@vmserver "tail -f /tmp/bench.log"

# Dump results
bash objstrd/benchmarks/rclone/dump_results.sh
```

Results are saved to `/tmp/rclone_bench/{backend}/` on the VM.

---

## Results

> Server and rclone client are **co-located on the same VM** (loopback only  - 
> no real network hop). Numbers reflect storage + HTTP stack overhead, not
> network bandwidth.  
> **Quick mode:** 5 s/phase, file-cap 1 for large objects, 30 s mixed workload.

rclone v1.73.3 . objstrd latest build . `--backends mem,fs,raw-file`

Raw backend image: 2 GiB file on the VM's root filesystem (`/tmp`).
Flush interval: 2 s.

---

#### P2  -  Small objects (8 MiB, 4 threads, single PUT)

| Backend | Upload | Download |
|---------|--------|----------|
| mem | 335 MiB/s | 1.1 GiB/s |
| fs | 357 MiB/s | 135 MiB/s |
| raw-file | 330 MiB/s | 644 MiB/s |

<details>
<summary>All sizes (P1 1t + P2 4t)</summary>

| Size | Threads | Backend | Upload | Download |
|------|---------|---------|--------|----------|
| 4 KiB | 1 | mem | 2.2 MiB/s | 2.9 MiB/s |
| 4 KiB | 1 | fs | 1.0 MiB/s | 87 KiB/s |
| 4 KiB | 1 | raw-file | 1.3 MiB/s | 2.2 MiB/s |
| 1 MiB | 1 | mem | 122 MiB/s | 288 MiB/s |
| 1 MiB | 1 | fs | 106 MiB/s | 19 MiB/s |
| 1 MiB | 1 | raw-file | 77 MiB/s | 64 MiB/s |
| 8 MiB | 1 | mem | 169 MiB/s | 431 MiB/s |
| 8 MiB | 1 | fs | 158 MiB/s | 45 MiB/s |
| 8 MiB | 1 | raw-file | 153 MiB/s | 247 MiB/s |
| 4 KiB | 4 | mem | 4.5 MiB/s | 5.8 MiB/s |
| 4 KiB | 4 | fs | 973 KiB/s | 264 KiB/s |
| 4 KiB | 4 | raw-file | 4.3 MiB/s | 3.7 MiB/s |
| 1 MiB | 4 | mem | 319 MiB/s | 690 MiB/s |
| 1 MiB | 4 | fs | 182 MiB/s | 43 MiB/s |
| 1 MiB | 4 | raw-file | 200 MiB/s | 157 MiB/s |
| 8 MiB | 4 | mem | 335 MiB/s | 1.1 GiB/s |
| 8 MiB | 4 | fs | 357 MiB/s | 135 MiB/s |
| 8 MiB | 4 | raw-file | 330 MiB/s | 644 MiB/s |

</details>

---

#### P4  -  Large objects, single PUT (4 threads)

| Size | Backend | Upload | Download |
|------|---------|--------|----------|
| 64 MiB | mem | 150 MiB/s | 510 MiB/s |
| 64 MiB | fs | 113 MiB/s | 53 MiB/s |
| 64 MiB | raw-file | 102 MiB/s | 400 MiB/s |
| 128 MiB | mem | 134 MiB/s | 456 MiB/s |
| 128 MiB | fs | 116 MiB/s | 61 MiB/s |
| 128 MiB | raw-file | 75 MiB/s | 481 MiB/s |
| 256 MiB | mem | 69 MiB/s | 363 MiB/s |
| 256 MiB | fs | 79 MiB/s | 561 MiB/s |
| 256 MiB | raw-file | 66 MiB/s | 267 MiB/s |
| 512 MiB | mem | 73 MiB/s | 425 MiB/s |
| 512 MiB | fs | 65 MiB/s | 249 MiB/s |
| 512 MiB | raw-file | 39 MiB/s | 134 MiB/s |

512 MiB confirms the tempfile-spooled PUT path works without OOM on a 4 GB
VM.  Prior to the streaming fix the server would OOM-kill at this size.
raw-file P3 (1t) ran successfully; P4 (4t) was not tested because the 2 GiB
image filled up after three P3 iterations.  fs P3 (1t) was again skipped by
rclone's calibration budget.

<details>
<summary>P3 single-thread results</summary>

| Size | Backend | Upload | Download |
|------|---------|--------|----------|
| 64 MiB | mem | 156 MiB/s | 482 MiB/s |
| 64 MiB | fs | 146 MiB/s | 61 MiB/s |
| 64 MiB | raw-file | 173 MiB/s | 482 MiB/s |
| 128 MiB | mem | 164 MiB/s | 428 MiB/s |
| 128 MiB | fs | 96 MiB/s | 78 MiB/s |
| 128 MiB | raw-file | 157 MiB/s | 397 MiB/s |
| 256 MiB | mem | 74 MiB/s | 379 MiB/s |
| 256 MiB | fs | 67 MiB/s | 192 MiB/s |
| 256 MiB | raw-file | 56 MiB/s | 263 MiB/s |
| 512 MiB | raw-file | 39 MiB/s | 134 MiB/s |

fs 128/256 MiB retested with 15 s/phase.  512 MiB P3 only available for
raw-file (mem and fs skipped by rclone calibration).

</details>

---

#### P6  -  Large objects, multipart (16 Mi chunks, 4 threads)

| Size | Backend | Upload | Download |
|------|---------|--------|----------|
| 64 MiB | mem | 67 MiB/s | 450 MiB/s |
| 64 MiB | fs | 63 MiB/s | 61 MiB/s |
| 64 MiB | raw-file | 60 MiB/s | 268 MiB/s |
| 128 MiB | mem | 64 MiB/s | 421 MiB/s |
| 128 MiB | fs | 64 MiB/s | 62 MiB/s |
| 128 MiB | raw-file | 41 MiB/s | 257 MiB/s |
| 256 MiB | mem | 66 MiB/s | 336 MiB/s |
| 256 MiB | fs | 52 MiB/s | 274 MiB/s |
| 256 MiB | raw-file | 51 MiB/s | 178 MiB/s |

<details>
<summary>P5 single-thread multipart results</summary>

| Size | Backend | Upload | Download |
|------|---------|--------|----------|
| 64 MiB | mem | 74 MiB/s | 410 MiB/s |
| 64 MiB | fs | 66 MiB/s | 62 MiB/s |
| 64 MiB | raw-file | 67 MiB/s | 413 MiB/s |
| 128 MiB | mem | 88 MiB/s | 480 MiB/s |
| 128 MiB | raw-file | 59 MiB/s | 335 MiB/s |
| 256 MiB | mem | 75 MiB/s | 343 MiB/s |
| 256 MiB | raw-file | 62 MiB/s | 235 MiB/s |

</details>

---

#### P7  -  Mixed concurrent workload (30 s, flush every 2 s)

Three rclone processes running simultaneously:
- **tiny**: 4 KiB files, 16 threads, single PUT
- **medium**: 1 MiB files, 8 threads, single PUT
- **large**: 64-256 MiB files, 2 threads, multipart (16 Mi chunks)

| Tier | Backend | Upload | Download |
|------|---------|--------|----------|
| tiny (4 KiBx16t) | mem | 4.7 MiB/s | 2.8 MiB/s |
| tiny (4 KiBx16t) | fs | 2.0 MiB/s | 1.2 MiB/s |
| tiny (4 KiBx16t) | raw-file | 1.5 MiB/s | 1.4 MiB/s |
| medium (1 MiBx8t) | mem | 256 MiB/s | 584 MiB/s |
| medium (1 MiBx8t) | fs | 137 MiB/s | 71 MiB/s |
| medium (1 MiBx8t) | raw-file | 153 MiB/s | 158 MiB/s |
| large (256 Mix2t mp) | mem | 72 MiB/s | 423 MiB/s |
| large (256 Mix2t mp) | fs | 72 MiB/s | 379 MiB/s |
| large (256 Mix2t mp) | raw-file | 84 MiB/s | 289 MiB/s |

The raw-file backend sustained ~15 flush cycles during the 30 s run with no
errors or stalls  -  confirming flush-under-load correctness.

---

#### Backend comparison summary (256 MiB, 4 threads)

| Backend | Single PUT upload | Multipart upload | Single PUT download |
|---------|-----------------|-----------------|---------------------|
| mem | 69 MiB/s | 66 MiB/s | 363 MiB/s |
| fs | 79 MiB/s | 52 MiB/s | 561 MiB/s |
| raw-file | 66 MiB/s | 51 MiB/s | 267 MiB/s |

#### 512 MiB single PUT (4 threads where available)

| Backend | Threads | Upload | Download |
|---------|---------|--------|----------|
| mem | 4 | 73 MiB/s | 425 MiB/s |
| fs | 4 | 65 MiB/s | 249 MiB/s |
| raw-file | 1 | 39 MiB/s | 134 MiB/s |

No OOM kill  -  the tempfile-spooled `put_object` path handles 512 MiB objects
on a 4 GB VM without buffering the entire body in RAM.

> Remaining backends (`raw-file-dio`, `raw-dev`, `raw-dev-dio`, `raw-loop`,
> `raw-loop-dio`)  -  results pending.

---

### Observations

- **`fs` write speeds are inflated by the Linux page cache.** The `fs`
  backend writes one file per object to the ext4 root filesystem. For
  objects that fit in RAM the kernel buffers the write in the page cache
  and returns immediately  -  the data has not yet reached disk. This
  makes `fs` upload numbers appear competitive with `mem` for small/medium
  objects, but the cost shifts to fsync and writeback later. The `raw-file`
  and `raw-dev` backends write through their own flush loop and reflect
  actual storage throughput more honestly.
- **`fs` large-object download also benefits from the page cache.** The
  561 MiB/s download for 256 MiB (P4, 4t) exceeds `mem` (363 MiB/s)
  because the file was just written and is still hot in the page cache  - 
  the read is pure memory-to-memory with no disk I/O. This number does
  not reflect cold-cache or steady-state performance.
- **ext4 metadata overhead dominates `fs` small-object reads.** Listing,
  opening, and stat-ing thousands of individual files on ext4 is far
  slower than the raw backend's single-file index lookup. This is why
  `fs` download drops to 45-135 MiB/s while raw-file sustains 157-644
  MiB/s for the same sizes.
- **Tiny-object throughput** remains HTTP-overhead-bound at ~1-5 MiB/s
  across backends (~250-1250 req/s).
- **512 MiB single PUT works without OOM.** The server's `put_object`
  handler now spools large bodies through a tempfile instead of buffering
  the entire request in RAM. Objects <= 8 MiB still use an in-memory
  fast path. This means 512 MiB (and larger) single PUTs succeed on a
  4 GB VM without hitting the OOM killer.
- All checksums verified by rclone (`0 differences found`) on every cycle.
