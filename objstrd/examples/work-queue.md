# Example: Data-Local Work Queue with objstrd

Use objstrd as a work distribution layer where each worker machine runs
its own objstrd instance. A dispatcher pushes work items to nodes; a
simple script on each node watches the event socket, reads the object
locally, runs a command, and deletes it. Workers need zero cluster
awareness -- they just process whatever appears.

---

## 1. Simple Worker (bash + socat)

The minimal version. Each node runs objstrd and a bash loop. The
processing command does not need to know about S3, objstrd, or the
cluster -- it just receives a local file path.

### Architecture

```
                    +-----------------+
  Any source ------>| Dispatcher      |
  (Kinesis, Kafka,  | picks node,     |
   SQS, HTTP, ...)  | does S3 PUT     |
                    +----+----+----+--+
                         |    |    |
                    PUT to one node (RF=1)
                         |    |    |
               +---------+    |    +---------+
               v              v              v
          Node 0         Node 1         Node 2
          [objstrd]      [objstrd]      [objstrd]
          event socket   event socket   event socket
               |              |              |
          worker.sh      worker.sh      worker.sh
          reads local    reads local    reads local
          runs command   runs command   runs command
          deletes        deletes        deletes
```

Each worker is a self-contained loop. It has no idea other nodes exist.

### Node Setup

```bash
objstrd \
  --image /data/work-queue.raw \
  --size-mb 10240 \
  --port 8080 \
  --event-socket /tmp/objstrd.sock \
  --event-secret work-secret-1234
```

### Worker Script

This is all a node needs. It watches for PUTs, downloads the object
locally, runs your processing command, and deletes the object:

```bash
#!/usr/bin/env bash
# worker.sh -- process objects as they arrive on the local objstrd
#
# Usage: ./worker.sh [process_command]
# Default: just prints object size. Replace with your pipeline step.

set -euo pipefail

ENDPOINT="http://localhost:8080"
BUCKET="work"
SOCK="/tmp/objstrd.sock"
SECRET="work-secret-1234"
WORKDIR=$(mktemp -d)
PROCESS_CMD="${1:-default_process}"

trap 'rm -rf "$WORKDIR"' EXIT

default_process() {
    local key="$1" file="$2"
    echo "processed $key ($(stat -c%s "$file") bytes)"
}

# Connect to event socket via socat, authenticate
FIFO=$(mktemp -u /tmp/worker_fifo.XXXXXX)
mkfifo "$FIFO"
(echo "SECRET $SECRET"; cat) | socat - UNIX-CONNECT:"$SOCK" > "$FIFO" &
SOCAT_PID=$!
trap 'kill $SOCAT_PID 2>/dev/null; rm -rf "$WORKDIR" "$FIFO"' EXIT

read -r RESP < "$FIFO"
if [ "$RESP" != "OK" ]; then
    echo "Auth failed: $RESP" >&2
    exit 1
fi

echo "Watching for work on $SOCK ..."

while read -r TYPE PAYLOAD; do
    case "$TYPE" in
        PUT)
            # Download object locally (zero network hop)
            OUTFILE="$WORKDIR/$(basename "$PAYLOAD")"
            if curl -sf "$ENDPOINT/$BUCKET/$PAYLOAD" -o "$OUTFILE"; then
                # Run the processing step
                "$PROCESS_CMD" "$PAYLOAD" "$OUTFILE"

                # Done -- delete it
                curl -sf -X DELETE "$ENDPOINT/$BUCKET/$PAYLOAD" > /dev/null
                rm -f "$OUTFILE"
            fi
            ;;
    esac
done < "$FIFO"
```

Use it with any command:

```bash
# Default -- just log
./worker.sh

# Run ffmpeg on each object
./worker.sh transcode_video

# Feed to a Python ML script
./worker.sh "python3 infer.py"
```

The processing command receives two arguments: the object key and a local
file path. It does not need to know about S3, objstrd, or the cluster.

### Dispatcher (Python)

Pushes work items to nodes. The only part that knows the topology:

```python
#!/usr/bin/env python3
"""Push work items to the least-full objstrd node."""

import sys
import requests
import boto3
from botocore.config import Config

NODES = [
    "http://node-0:8080",
    "http://node-1:8080",
    "http://node-2:8080",
]

clients = [
    boto3.client(
        "s3",
        endpoint_url=url,
        aws_access_key_id="unused",
        aws_secret_access_key="unused",
        config=Config(signature_version="s3v4"),
    )
    for url in NODES
]


def pick_node():
    """Pick the node with the most free space."""
    best, best_free = 0, -1
    for i, url in enumerate(NODES):
        try:
            info = requests.get(f"{url}/_admin/info", timeout=2).json()
            free = info.get("free_bytes", 0)
            if free > best_free:
                best_free = free
                best = i
        except Exception:
            continue
    return best


def dispatch(key, data):
    """Send one work item to the best available node."""
    node = pick_node()
    clients[node].put_object(Bucket="work", Key=key, Body=data)


# Example: dispatch files from the command line
if __name__ == "__main__":
    for path in sys.argv[1:]:
        with open(path, "rb") as f:
            dispatch(f"jobs/{path}", f.read())
        print(f"dispatched {path}")
```

### Design Notes

- **Workers are not cluster-aware.** They watch a local socket and
  process local files. Your processing command can be a shell script,
  a Python program, a compiled binary -- anything that takes a file path.

- **Each object is an atomic work unit.** No coordination between nodes
  is needed. If a worker crashes mid-processing, the object is still
  on disk; on restart the worker can LIST the `jobs/` prefix and
  reprocess anything not yet deleted.

- **Backpressure is automatic.** When a node's store fills up, the
  dispatcher's `pick_node()` stops routing to it. Objects only
  accumulate on slow nodes.

- **Any input source works.** The dispatcher is just a PUT. Feed it
  from Kinesis, Kafka, SQS, a directory watcher, cron, a webhook,
  or a simple `for` loop.

- **Scaling.** Add a machine, start objstrd and worker.sh, tell the
  dispatcher about the new endpoint. No reconfiguration of existing
  nodes.

---

## 2. Ray Cluster Integration

The same pattern scaled up with Ray. Each Ray node runs objstrd.
A dispatcher streams work from Kinesis (or any source) to nodes.
Ray workers watch the local event socket and process objects with
zero network hops on the read path.

### Architecture

```
                    +-----------------+
  Kinesis --------->| Dispatcher      |
  (or any source)   | picks target    |
                    | node, does PUT  |
                    +----+----+----+--+
                         |    |    |
                    PUT to exactly one node (RF=1)
                         |    |    |
               +---------+    |    +---------+
               v              v              v
          Ray Node 0     Ray Node 1     Ray Node 2
          [objstrd]      [objstrd]      [objstrd]
          event socket   event socket   event socket
               |              |              |
          Ray worker     Ray worker     Ray worker
          watches PUT    watches PUT    watches PUT
          events,        events,        events,
          reads local,   reads local,   reads local,
          processes      processes      processes
               |              |              |
               +------+-------+------+------+
                      |              |
                      v              v
               DELETE from local store after processing
```

### Kinesis Dispatcher

Consumes from a Kinesis stream and distributes work items across nodes:

```python
#!/usr/bin/env python3
"""Dispatch Kinesis records to Ray/objstrd nodes, least-busy-first."""

import time
import boto3
import requests
from botocore.config import Config

KINESIS_STREAM = "work-queue"
NODES = [
    "http://ray-node-0:8080",
    "http://ray-node-1:8080",
    "http://ray-node-2:8080",
]

kinesis = boto3.client("kinesis", region_name="us-east-1")

# One S3 client per node
node_clients = []
for url in NODES:
    c = boto3.client(
        "s3",
        endpoint_url=url,
        aws_access_key_id="unused",
        aws_secret_access_key="unused",
        config=Config(signature_version="s3v4"),
    )
    node_clients.append(c)


def pick_node():
    """Pick the node with the most free space via /_admin/info."""
    best_idx, best_free = 0, -1
    for i, url in enumerate(NODES):
        try:
            info = requests.get(f"{url}/_admin/info", timeout=2).json()
            free = info.get("free_bytes", 0)
            if free > best_free:
                best_free = free
                best_idx = i
        except Exception:
            continue
    return best_idx


def consume():
    desc = kinesis.describe_stream(StreamName=KINESIS_STREAM)
    shards = desc["StreamDescription"]["Shards"]

    for shard in shards:
        shard_id = shard["ShardId"]
        resp = kinesis.get_shard_iterator(
            StreamName=KINESIS_STREAM,
            ShardId=shard_id,
            ShardIteratorType="TRIM_HORIZON",
        )
        shard_iter = resp["ShardIterator"]

        while shard_iter:
            out = kinesis.get_records(ShardIterator=shard_iter, Limit=500)
            for rec in out["Records"]:
                node_idx = pick_node()
                key = f"jobs/{shard_id}/{rec['SequenceNumber']}"
                node_clients[node_idx].put_object(
                    Bucket="work", Key=key, Body=rec["Data"]
                )
            shard_iter = out.get("NextShardIterator")
            if not out["Records"]:
                time.sleep(1)


if __name__ == "__main__":
    consume()
```

### Ray Worker

Runs on each node, watches the local event socket, processes objects:

```python
#!/usr/bin/env python3
"""Ray worker: watch local objstrd event socket, process new objects."""

import socket
import boto3
from botocore.config import Config

LOCAL_OBJSTRD = "http://localhost:8080"
SOCK_PATH = "/tmp/objstrd.sock"
SECRET = "work-secret-1234"

local_s3 = boto3.client(
    "s3",
    endpoint_url=LOCAL_OBJSTRD,
    aws_access_key_id="unused",
    aws_secret_access_key="unused",
    config=Config(signature_version="s3v4"),
)


def process_object(key, data):
    """Your processing logic here."""
    print(f"Processing {key} ({len(data)} bytes)")
    # ... inference, ETL, aggregation, etc.


def watch_and_process():
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(SOCK_PATH)
    sock.sendall(f"SECRET {SECRET}\n".encode())

    buf = b""
    while not buf.endswith(b"\n"):
        buf += sock.recv(1)
    if buf.decode().strip() != "OK":
        raise RuntimeError(f"Auth failed: {buf.decode().strip()}")

    print(f"Connected to {SOCK_PATH}, waiting for work...")

    remainder = b""
    while True:
        data = sock.recv(4096)
        if not data:
            break
        remainder += data
        while b"\n" in remainder:
            line, remainder = remainder.split(b"\n", 1)
            msg = line.decode().strip()
            if not msg:
                continue

            parts = msg.split(" ", 1)
            event_type = parts[0]
            payload = parts[1] if len(parts) > 1 else ""

            if event_type == "PUT" and payload.startswith("jobs/"):
                # Read from local store -- zero network hop
                resp = local_s3.get_object(Bucket="work", Key=payload)
                body = resp["Body"].read()

                process_object(payload, body)

                # Delete locally after processing
                local_s3.delete_object(Bucket="work", Key=payload)
                print(f"  Done: {payload}")


if __name__ == "__main__":
    watch_and_process()
```

### Ray-Specific Notes

- **Node selection strategies:**
  - *Least free space* -- default `select_targets` behavior, good baseline
  - *Round robin* -- simplest, ignores node load
  - *Least in-flight* -- dispatcher tracks PUT count minus DELETE count
  - *Ray-aware* -- query Ray's scheduling API for node utilization

- **Fault tolerance.** If a node dies, its unprocessed objects are still
  on its local image. On restart, LIST the `jobs/` prefix and reprocess
  anything not yet deleted.

- **The hot path (read + process) is always local.** Only the initial
  PUT crosses the network.
