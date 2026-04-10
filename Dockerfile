# ============================================================
# Dockerfile -- multi-stage build for the objstr workspace
#
# Produces a minimal image containing four binaries:
#   objstrd              S3-compatible object store daemon
#   rawobjstr            Raw block device CLI tool
#   shardedobjstr        Sharded object store CLI
#   shardedobjstr-catview  Catalog viewer CLI
#
# Build:
#   docker build -t objstr .
#
# Run (standalone mode, file-backed):
#   docker run --rm -p 8000:8000 objstr \
#     --image /data/store.raw --size-mb 512 --port 8000
#
# Run with a block device:
#   docker run --rm --privileged --device /dev/nvme0n1 -p 8000:8000 \
#     objstr --image /dev/nvme0n1 --port 8000
#
# Run a local cluster:
#   docker compose up --build
# ============================================================

# ------------------ builder stage ------------------
FROM rust:1-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Copy manifests and lockfile first for better layer caching.
# When only source files change, Cargo can reuse the dependency build cache.
COPY Cargo.toml Cargo.lock* ./
COPY rawobjstr/Cargo.toml         rawobjstr/Cargo.toml
COPY rawobjstr/python/Cargo.toml  rawobjstr/python/Cargo.toml
COPY shardedobjstr/Cargo.toml         shardedobjstr/Cargo.toml
COPY shardedobjstr/python/Cargo.toml  shardedobjstr/python/Cargo.toml
COPY objstrd/Cargo.toml            objstrd/Cargo.toml

# Create stub lib/main files so `cargo build` can resolve the workspace and
# download + compile all dependencies before we copy the real source.
RUN mkdir -p rawobjstr/src/bin \
             rawobjstr/python/src \
             shardedobjstr/src/bin \
             shardedobjstr/python/src \
             objstrd/src/bin \
    && echo 'fn main(){}' > rawobjstr/src/bin/rawobjstr.rs \
    && echo 'fn main(){}' > shardedobjstr/src/bin/shardedobjstr.rs \
    && echo 'fn main(){}' > shardedobjstr/src/bin/shardedobjstr_catview.rs \
    && echo 'fn main(){}' > objstrd/src/bin/server.rs \
    && touch rawobjstr/src/lib.rs \
    && touch rawobjstr/python/src/lib.rs \
    && touch shardedobjstr/src/lib.rs \
    && touch shardedobjstr/python/src/lib.rs

# Copy build.rs files -- they need to compile but will be re-run with full
# source anyway. Also copy static HTML that objstrd/build.rs embeds.
COPY rawobjstr/build.rs            rawobjstr/build.rs
COPY rawobjstr/python/build.rs     rawobjstr/python/build.rs
COPY shardedobjstr/build.rs            shardedobjstr/build.rs
COPY shardedobjstr/python/build.rs     shardedobjstr/python/build.rs
COPY objstrd/build.rs              objstrd/build.rs
COPY objstrd/static/               objstrd/static/

# Pre-build dependencies only (stubs will fail to link but deps compile).
# Use `|| true` because the stub sources will cause build errors -- we only
# care about caching downloaded and compiled dependency crates.
RUN cargo build --release --workspace 2>/dev/null || true

# Now copy the full source tree. The .git directory is needed by build.rs
# to bake the git hash into each binary.
COPY . .

# Touch all source files so Cargo knows they changed (timestamps matter).
RUN find rawobjstr/src shardedobjstr/src objstrd/src \
        rawobjstr/python/src shardedobjstr/python/src \
        -name '*.rs' -exec touch {} +

# Build all release binaries. Only the four Rust binaries are needed;
# Python cdylib crates are excluded (they need maturin + Python).
RUN cargo build --release \
        --bin objstrd \
        --bin rawobjstr \
        --bin shardedobjstr \
        --bin shardedobjstr-catview

# Smoke-test: make sure binaries exist and are executable.
RUN ls -lh /src/target/release/objstrd \
           /src/target/release/rawobjstr \
           /src/target/release/shardedobjstr \
           /src/target/release/shardedobjstr-catview

# ------------------ runtime stage ------------------
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/target/release/objstrd              /usr/local/bin/
COPY --from=builder /src/target/release/rawobjstr            /usr/local/bin/
COPY --from=builder /src/target/release/shardedobjstr        /usr/local/bin/
COPY --from=builder /src/target/release/shardedobjstr-catview /usr/local/bin/

# Default data directory for file-backed stores.
RUN mkdir -p /data

EXPOSE 8000

ENTRYPOINT ["objstrd"]
CMD ["--help"]
