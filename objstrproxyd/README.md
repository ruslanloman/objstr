# objstrproxyd

Stateless S3 proxy that sits between clients and the objstrd cluster. It
resolves object placement from a coordinator, then splits reads into parallel
range requests across multiple replicas for higher throughput.

---

## Architecture

```
  S3 Client (GET /bucket/key)
        |
  +-----v-------------------------------+
  |  objstrproxyd (stateless, zone-aware)  |
  |                                      |
  |  1. Lookup: ask any coordinator     |
  |     "where is bucket/key?"          |
  |     => {nodes: [A,B,C], size: 1GB}  |
  |                                      |
  |  2. Plan: split byte range across   |
  |     replicas, prefer local zone     |
  |                                      |
  |  3. Fan-out: parallel S3 range GETs |
  |     Node A: bytes 0-333MB           |
  |     Node B: bytes 333MB-666MB       |
  |     Node C: bytes 666MB-1GB         |
  |                                      |
  |  4. Reassemble: stream to client    |
  |     in order as ranges complete     |
  +--+----------+----------+-----------+
     |          |          |
  +--v--+   +--v--+   +--v--+
  |NodeA|   |NodeB|   |NodeC|   (objstrd --mode node)
  | S3  |   | S3  |   | S3  |
  +-----+   +-----+   +-----+
```

---

## Why a Separate Crate

objstrproxyd has no storage, no RawObjectStore, no O_DIRECT, no index code. It
is a pure HTTP-to-HTTP proxy. Keeping it separate from objstrd means:

- **Small binary.** No block device dependencies. Deploys on lightweight
  edge boxes, ARM containers, small VMs with no disks.
- **Independent scaling.** Run N objstrproxyd behind a load balancer per zone.
  They share no state - scale horizontally without touching storage nodes.
- **Simpler code.** objstrd stays focused on storage + S3 serving.
  objstrproxyd stays focused on routing + fan-out.

---

## Core Design

### Placement Lookup

The objstrproxyd contacts any coordinator (round-robin or failover list) to
resolve an object key:

```
GET /_cluster/locate?bucket=X&key=Y  -->  coordinator

Response:
{
  "nodes": [
    {"id": "node-a", "addr": "10.0.1.1:8000", "zone": "us-east-1a"},
    {"id": "node-b", "addr": "10.0.2.1:8000", "zone": "us-west-2a"},
    {"id": "node-c", "addr": "10.0.3.1:8000", "zone": "eu-west-1a"}
  ],
  "size": 1073741824,
  "crc32c": "a1b2c3d4"
}
```

The coordinator endpoint (`/_cluster/locate`) needs to be added to objstrd
when running in coordinator mode. This is the only new API surface required.

### Small Object Short-Circuit

Objects below a configurable threshold (default 4 MB) are not worth
striping. The objstrproxyd proxies the entire GET to the nearest replica
(same zone preferred) as a simple reverse proxy. No fan-out overhead.

### Large Object Striping

For objects above the threshold:

1. Divide the byte range into N chunks where N = number of available
   replicas (or fewer if some replicas are in distant zones and zone
   preference is configured).
2. Issue parallel S3 `GET` requests with `Range: bytes=start-end` to
   each selected node.
3. Stream the response to the client in order. Start sending range 0
   bytes as soon as they arrive. Buffer subsequent ranges up to a
   configurable limit.

### Zone Awareness

Each objstrproxyd is configured with its own zone ID. When planning range
splits:

- **Local zone replicas** get a larger share of the byte range (lower
  latency, free bandwidth within the zone).
- **Remote zone replicas** get a smaller share or are used only as
  fallback.
- For small objects, read entirely from the local replica if available.

Example: object on 3 nodes, 1 local + 2 remote. Local gets 50% of the
range, each remote gets 25%.

### Failure Handling

- **Node down before request:** The coordinator's placement response
  includes all replicas. Skip unreachable nodes and redistribute their
  byte ranges to the remaining nodes.
- **Node fails mid-stream:** Retry the remaining bytes of that range
  against a different replica. The client sees a brief pause, not an
  error.
- **Coordinator unreachable:** Try the next coordinator in the list.
  If all coordinators are down, return 503 to the client.

### PUT Handling (Simple Pass-Through)

For writes, the objstrproxyd acts as a simple reverse proxy: forward the PUT
to the nearest storage node. The node handles coordinator notification
and replication as normal. No fan-out on writes - that complexity
belongs in the coordinator's replication logic.

Future optimization: the objstrproxyd could fan-out PUTs to multiple nodes
in parallel to pre-satisfy the replication factor, reducing the
coordinator's replication work. This interacts with the sync/async
replication mode.

---

## Configuration

```bash
# Minimal
COORDINATORS=10.0.0.1:8000,10.0.0.2:8000 \
ZONE=us-east-1a \
PORT=9000 \
  objstrproxyd

# All options
COORDINATORS=10.0.0.1:8000,10.0.0.2:8000  # comma-separated coordinator list
ZONE=us-east-1a                             # this objstrproxyd's zone ID
PORT=9000                                   # listen port
BIND=0.0.0.0                                # bind address
STRIPE_THRESHOLD=4194304                    # min object size for striping (4 MB)
MAX_BUFFER=67108864                         # max reassembly buffer (64 MB)
ACCESS_KEY=mykey                            # S3 auth (passed to backend nodes)
SECRET_KEY=mysecret                         # S3 auth (passed to backend nodes)
```

---

## Dependencies

Minimal dependency set - no storage crates:

| Dependency | Purpose |
|------------|---------|
| `hyper` / `tokio` | HTTP server + async runtime |
| `reqwest` or `hyper` client | HTTP client for coordinator + node requests |
| `serde` / `serde_json` | Parse coordinator placement responses |
| `clap` or env vars | Configuration |

No dependency on `rawobjstr`, `object_store`, `s3s`, or any block
device code.

---

## Coordinator API Required

The objstrproxyd depends on a placement lookup endpoint on the coordinator.
This needs to be added to objstrd's coordinator mode:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/_cluster/locate` | GET | Resolve object key to node list + size |
| `/_cluster/nodes` | GET | List all nodes with zone + health info |

These are internal cluster endpoints (not part of the S3 API) and should
be authenticated with the cluster RPC secret, not S3 credentials.

---

## Milestones

### F0: Skeleton

- [ ] Crate folder, Cargo.toml, PLAN.md
- [ ] Basic HTTP server that accepts S3 GET and returns 501
- [ ] Coordinator address config, health check ping

### F1: Simple Proxy

- [ ] Placement lookup from coordinator
- [ ] Proxy GET to a single node (nearest replica, no striping)
- [ ] Proxy PUT to nearest node (pass-through)
- [ ] Small object path working end-to-end

### F2: Range Striping

- [ ] Split large GETs into parallel range reads across replicas
- [ ] Streaming reassembly in byte order
- [ ] Configurable stripe threshold
- [ ] Backpressure when reassembly buffer is full

### F3: Zone Awareness

- [ ] Zone-weighted range distribution
- [ ] Prefer local zone for small objects
- [ ] Weighted split for large objects (local gets larger share)

### F4: Failure Handling

- [ ] Retry failed ranges on alternate replicas
- [ ] Coordinator failover (try next in list)
- [ ] Timeout and circuit breaker per node

---

## Open Questions

- **LIST operations:** Should the objstrproxyd proxy ListObjectsV2 to the
  coordinator or to a storage node? The coordinator has the full index,
  so it could answer directly. Storage nodes can also answer for their
  local objects. Coordinator is the natural choice.
- **Caching placement lookups:** For repeated reads of the same key, the
  objstrproxyd could cache the placement response briefly (TTL 5-10s). This
  avoids hitting the coordinator on every request. The cache is small
  (key -> node list) and short-lived.
- **HEAD requests:** Proxy to nearest replica or coordinator? Coordinator
  has size/metadata in its index - could answer without hitting a node.
- **Multipart downloads:** For very large objects, should the objstrproxyd
  expose a custom range-read API beyond standard S3, or is S3 Range
  header sufficient?


objstrproxyd layer could be used to provide:  IAM/auth/users, bucket policy enforcement, CORS, encryption (encrypt-before-store), object lock enforcement, metrics aggregation, access logging, SQL select.