#!/bin/bash
#
# standalone/run_tree_cli.sh
#
# E2E test for shardedobjstr CLI -- exercises EVERY subcommand.
# Tests put, get, delete, list, verify, health, report, info,
# format, version, check-config, add-shard, remove-shard,
# repair-replication, vacuum, list-deleted against a nested tree config
# and a flat config.
#
# Topology (all local, no objstrd):
#
#   root (rf=2)
#     raw  /tmp/tree_cli_img0.raw        <- branch 1: raw image
#     inner (rf=1)                       <- branch 2: nested sharded store
#       fs   /tmp/tree_cli_fs            <- sub-branch 1: filesystem store
#       raw  /tmp/tree_cli_img1.raw      <- sub-branch 2: raw image
#
# Every object written goes to both branches (rf=2).
#
# Usage:
#   bash run_tree_cli.sh
#   SHARDEDOBJSTR=/path/to/shardedobjstr bash run_tree_cli.sh
#
set -euo pipefail

SHARDEDOBJSTR="${SHARDEDOBJSTR:-$HOME/build-sharded/release/shardedobjstr}"
RAWOBJSTR="${RAWOBJSTR:-$HOME/build-rawobjstr/release/rawobjstr}"

IMG0=/tmp/tree_cli_img0.raw
IMG1=/tmp/tree_cli_img1.raw
FS_DIR=/tmp/tree_cli_fs
CONF=/tmp/tree_cli.conf
FLAT_CONF=/tmp/tree_cli_flat.conf
SIZE_BYTES=$((128 * 1024 * 1024))  # 128 MB per raw image

# Extra images for add-shard / remove-shard (need 4 raw shards minimum)
IMG_A=/tmp/tree_cli_imgA.raw
IMG_B=/tmp/tree_cli_imgB.raw
IMG_C=/tmp/tree_cli_imgC.raw
IMG_D=/tmp/tree_cli_imgD.raw
IMG_NEW=/tmp/tree_cli_imgNEW.raw

PASS=0
FAIL=0

# ---- helpers --------------------------------------------------------

cleanup() {
    rm -f "$IMG0" "$IMG1" "$CONF" "$FLAT_CONF"
    rm -f "$IMG_A" "$IMG_B" "$IMG_C" "$IMG_D" "$IMG_NEW"
    rm -rf "$FS_DIR"
    rm -f /tmp/tree_cli_*.tmp
    echo ""
    echo "======================================="
    echo "Results: $PASS passed, $FAIL failed"
    echo "======================================="
    [ "$FAIL" -eq 0 ] || exit 1
}
trap cleanup EXIT

pass() { echo "  PASS: $1"; PASS=$((PASS + 1)); }
fail() { echo "  FAIL: $1"; FAIL=$((FAIL + 1)); }

# run NAME CMD... -- pass if exit 0
run() {
    local name="$1"; shift
    if "$@" >/dev/null 2>&1; then
        pass "$name"
    else
        fail "$name"
    fi
}

# run_fail NAME CMD... -- pass if exit non-zero
run_fail() {
    local name="$1"; shift
    if "$@" >/dev/null 2>&1; then
        fail "$name (expected failure)"
    else
        pass "$name"
    fi
}

# run_output NAME CMD... -- capture stdout, pass if exit 0
run_output() {
    local name="$1"; shift
    local out
    if out=$("$@" 2>/dev/null); then
        echo "$out"
        pass "$name"
    else
        fail "$name"
        echo ""
    fi
}

# assert_contains OUTPUT NEEDLE NAME
assert_contains() {
    if echo "$1" | grep -q "$2"; then
        pass "$3"
    else
        fail "$3 (expected '$2' in output)"
    fi
}

# assert_not_contains OUTPUT NEEDLE NAME
assert_not_contains() {
    if echo "$1" | grep -q "$2"; then
        fail "$3 (unexpected '$2' in output)"
    else
        pass "$3"
    fi
}

# assert_eq ACTUAL EXPECTED NAME
assert_eq() {
    if [ "$1" = "$2" ]; then
        pass "$3"
    else
        fail "$3 (got '$1', expected '$2')"
    fi
}

echo "=== shardedobjstr CLI tests (tree + flat) ==="
echo "Binary: $SHARDEDOBJSTR"
echo ""

# ===================================================================
# PART 1: version
# ===================================================================

echo "--- version ---"
OUT=$("$SHARDEDOBJSTR" --version 2>&1)
assert_contains "$OUT" "shardedobjstr" "version contains binary name"
assert_contains "$OUT" "git" "version contains git hash"

OUT=$("$SHARDEDOBJSTR" -V 2>&1)
assert_contains "$OUT" "shardedobjstr" "-V flag works"

OUT=$("$SHARDEDOBJSTR" version 2>&1)
assert_contains "$OUT" "shardedobjstr" "version subcommand works"

# ===================================================================
# PART 2: format (via shardedobjstr format --shards)
# ===================================================================

echo ""
echo "--- format ---"

rm -f "$IMG0" "$IMG1"
rm -rf "$FS_DIR"

# Format using shardedobjstr's own format command (flat --shards mode)
OUT=$(run_output "format 2 raw shards" \
    "$SHARDEDOBJSTR" format --shards "$IMG0,$IMG1" --size "$SIZE_BYTES" --replicas 2)
assert_contains "$OUT" "2 shards formatted" "format reports 2 shards formatted"

# Verify they were created and are usable
run "info on formatted shards" \
    "$SHARDEDOBJSTR" info --shards "$IMG0,$IMG1"

mkdir -p "$FS_DIR"

echo "Format OK."

# ===================================================================
# PART 3: check-config (flat config only)
# ===================================================================

echo ""
echo "--- check-config ---"

# Write a valid flat config
cat > "$FLAT_CONF" <<FLATEOF
replicas 2
shard raw $IMG0
shard raw $IMG1
FLATEOF

OUT=$("$SHARDEDOBJSTR" check-config --config "$FLAT_CONF" 2>&1)
assert_contains "$OUT" "Config OK" "check-config accepts valid flat config"

# Write an invalid flat config (zero replicas)
cat > /tmp/tree_cli_bad.tmp <<'BADEOF'
replicas 0
shard raw /tmp/nosuchfile.raw
BADEOF

run_fail "check-config rejects invalid config" \
    "$SHARDEDOBJSTR" check-config --config /tmp/tree_cli_bad.tmp

# ===================================================================
# PART 4: tree config -- write config and verify it loads
# ===================================================================

echo ""
echo "--- tree config setup ---"

cat > "$CONF" <<CONF
# tree CLI test -- nested sharded topology
cluster  tree-cli-test

root  rf=2
  raw  $IMG0
  inner  rf=1
    fs   $FS_DIR
    raw  $IMG1
CONF

echo "Config:"
cat "$CONF"
echo ""

# ===================================================================
# PART 5: info
# ===================================================================

echo "--- info ---"
OUT=$(run_output "info" "$SHARDEDOBJSTR" info --config "$CONF")
assert_contains "$OUT" "Cluster" "info shows cluster details"

# ===================================================================
# PART 6: put
# ===================================================================

echo ""
echo "--- put ---"

echo "hello world" > /tmp/tree_cli_put1.tmp
echo "second file content" > /tmp/tree_cli_put2.tmp
echo "third object data here" > /tmp/tree_cli_put3.tmp
dd if=/dev/urandom bs=4096 count=10 of=/tmp/tree_cli_put4.tmp 2>/dev/null

run "put hello.txt" \
    "$SHARDEDOBJSTR" put --config "$CONF" --key "hello.txt" --from /tmp/tree_cli_put1.tmp

run "put docs/readme.md" \
    "$SHARDEDOBJSTR" put --config "$CONF" --key "docs/readme.md" --from /tmp/tree_cli_put2.tmp

run "put data/file3.bin" \
    "$SHARDEDOBJSTR" put --config "$CONF" --key "data/file3.bin" --from /tmp/tree_cli_put3.tmp

run "put data/large.bin (40KB)" \
    "$SHARDEDOBJSTR" put --config "$CONF" --key "data/large.bin" --from /tmp/tree_cli_put4.tmp

# ===================================================================
# PART 7: list
# ===================================================================

echo ""
echo "--- list ---"

OUT=$(run_output "list all" "$SHARDEDOBJSTR" list --config "$CONF")
assert_contains "$OUT" "hello.txt" "list shows hello.txt"
assert_contains "$OUT" "docs/readme.md" "list shows docs/readme.md"
assert_contains "$OUT" "data/file3.bin" "list shows data/file3.bin"
assert_contains "$OUT" "data/large.bin" "list shows data/large.bin"
assert_contains "$OUT" "4 objects" "list shows 4 objects"

# list with prefix
OUT=$(run_output "list --prefix data/" "$SHARDEDOBJSTR" list --config "$CONF" --prefix "data/")
assert_contains "$OUT" "data/file3.bin" "prefix list has data/file3.bin"
assert_contains "$OUT" "data/large.bin" "prefix list has data/large.bin"
assert_not_contains "$OUT" "hello.txt" "prefix list excludes hello.txt"

# list long form
OUT=$(run_output "list --long" "$SHARDEDOBJSTR" list --config "$CONF" --long)
assert_contains "$OUT" "hello.txt" "long list has hello.txt"
assert_contains "$OUT" "shards:" "long list shows shard placement"

# ls alias
OUT=$(run_output "ls alias" "$SHARDEDOBJSTR" ls --config "$CONF")
assert_contains "$OUT" "hello.txt" "ls alias works"

# ===================================================================
# PART 8: get
# ===================================================================

echo ""
echo "--- get ---"

"$SHARDEDOBJSTR" get --config "$CONF" --key "hello.txt" --to /tmp/tree_cli_get1.tmp 2>/dev/null
GOT=$(cat /tmp/tree_cli_get1.tmp)
assert_eq "$GOT" "hello world" "get hello.txt content matches"

"$SHARDEDOBJSTR" get --config "$CONF" --key "docs/readme.md" --to /tmp/tree_cli_get2.tmp 2>/dev/null
GOT=$(cat /tmp/tree_cli_get2.tmp)
assert_eq "$GOT" "second file content" "get docs/readme.md content matches"

# get large binary and compare
"$SHARDEDOBJSTR" get --config "$CONF" --key "data/large.bin" --to /tmp/tree_cli_get4.tmp 2>/dev/null
if cmp -s /tmp/tree_cli_put4.tmp /tmp/tree_cli_get4.tmp; then
    pass "get data/large.bin binary matches"
else
    fail "get data/large.bin binary mismatch"
fi

# get missing key should fail
run_fail "get missing key fails" \
    "$SHARDEDOBJSTR" get --config "$CONF" --key "no-such-key.txt" --to /tmp/tree_cli_gone.tmp

# ===================================================================
# PART 9: verify
# ===================================================================

echo ""
echo "--- verify ---"

OUT=$("$SHARDEDOBJSTR" verify --config "$CONF" 2>&1)
assert_contains "$OUT" "CLEAN" "verify reports CLEAN shards"
assert_contains "$OUT" "consistent" "verify shows consistency check"

# ===================================================================
# PART 10: health
# ===================================================================

echo ""
echo "--- health ---"

OUT=$("$SHARDEDOBJSTR" health --config "$CONF" 2>&1)
assert_contains "$OUT" "HEALTHY" "health shows HEALTHY"
assert_contains "$OUT" "node:inner" "health shows inner node"

# ===================================================================
# PART 11: report
# ===================================================================

echo ""
echo "--- report ---"

OUT=$("$SHARDEDOBJSTR" report --config "$CONF" 2>&1)
assert_contains "$OUT" "hello.txt" "report lists hello.txt"
assert_contains "$OUT" "data/file3.bin" "report lists data/file3.bin"

OUT=$("$SHARDEDOBJSTR" report --config "$CONF" --under-replicated 2>&1)
assert_contains "$OUT" "All objects meet replication factor" "no under-replicated objects"

# ===================================================================
# PART 12: overwrite
# ===================================================================

echo ""
echo "--- overwrite ---"

echo "updated content" > /tmp/tree_cli_put_ow.tmp
run "put overwrite hello.txt" \
    "$SHARDEDOBJSTR" put --config "$CONF" --key "hello.txt" --from /tmp/tree_cli_put_ow.tmp

"$SHARDEDOBJSTR" get --config "$CONF" --key "hello.txt" --to /tmp/tree_cli_get_ow.tmp 2>/dev/null
GOT=$(cat /tmp/tree_cli_get_ow.tmp)
assert_eq "$GOT" "updated content" "overwritten hello.txt has new content"

# ===================================================================
# PART 13: delete
# ===================================================================

echo ""
echo "--- delete ---"

run "delete docs/readme.md" \
    "$SHARDEDOBJSTR" delete --config "$CONF" --key "docs/readme.md"

# del alias
run "del alias data/file3.bin" \
    "$SHARDEDOBJSTR" del --config "$CONF" --key "data/file3.bin"

# verify they are gone
OUT=$("$SHARDEDOBJSTR" list --config "$CONF" 2>/dev/null)
assert_not_contains "$OUT" "docs/readme.md" "deleted readme gone from list"
assert_not_contains "$OUT" "data/file3.bin" "deleted file3 gone from list"

# get should fail
run_fail "get deleted key fails" \
    "$SHARDEDOBJSTR" get --config "$CONF" --key "docs/readme.md" --to /tmp/tree_cli_gone.tmp

# remaining objects still accessible
"$SHARDEDOBJSTR" get --config "$CONF" --key "data/large.bin" --to /tmp/tree_cli_surv.tmp 2>/dev/null
if cmp -s /tmp/tree_cli_put4.tmp /tmp/tree_cli_surv.tmp; then
    pass "surviving object data intact"
else
    fail "surviving object data changed"
fi

# ===================================================================
# PART 14: list-deleted
# ===================================================================

echo ""
echo "--- list-deleted ---"

OUT=$("$SHARDEDOBJSTR" list-deleted --config "$CONF" 2>&1)
assert_contains "$OUT" "docs/readme.md" "list-deleted shows readme"
assert_contains "$OUT" "data/file3.bin" "list-deleted shows file3"
assert_contains "$OUT" "delete marker" "list-deleted shows marker count"

# ===================================================================
# PART 15: vacuum
# ===================================================================

echo ""
echo "--- vacuum ---"

OUT=$("$SHARDEDOBJSTR" vacuum --config "$CONF" 2>&1)
assert_contains "$OUT" "Vacuum complete" "vacuum completed"

# after vacuum, list-deleted should be empty
OUT=$("$SHARDEDOBJSTR" list-deleted --config "$CONF" 2>&1)
assert_contains "$OUT" "No delete markers" "no markers after vacuum"

# ===================================================================
# PART 17: repair-replication
# ===================================================================

echo ""
echo "--- repair-replication ---"

# put some objects first to have something for repair-replication
for i in $(seq 1 10); do
    echo "rebal-data-$i" > /tmp/tree_cli_rebal_${i}.tmp
    "$SHARDEDOBJSTR" put --config "$CONF" --key "rebal/obj-${i}.txt" \
        --from /tmp/tree_cli_rebal_${i}.tmp >/dev/null 2>&1
done

OUT=$("$SHARDEDOBJSTR" repair-replication --config "$CONF" --batch-size 50 2>&1)
assert_contains "$OUT" "Repair-replication:" "repair-replication starts"
assert_contains "$OUT" "Re-replicated:" "repair-replication reports re-replicated"
assert_contains "$OUT" "Trimmed:" "repair-replication reports trimmed"

# ===================================================================
# PART 18: batch put/delete cycle
# ===================================================================

echo ""
echo "--- batch put ---"

for i in $(seq 1 10); do
    echo "batch-data-$i" > /tmp/tree_cli_batch_${i}.tmp
    run "put batch/obj-${i}.txt" \
        "$SHARDEDOBJSTR" put --config "$CONF" --key "batch/obj-${i}.txt" --from /tmp/tree_cli_batch_${i}.tmp
done

OUT=$("$SHARDEDOBJSTR" list --config "$CONF" --prefix "batch/" 2>/dev/null)
BATCH_COUNT=$(echo "$OUT" | grep -c "batch/" || true)
assert_eq "$BATCH_COUNT" "10" "10 batch objects listed"

echo ""
echo "--- batch delete ---"

for i in 1 3 5 7 9; do
    run "delete batch/obj-${i}.txt" \
        "$SHARDEDOBJSTR" delete --config "$CONF" --key "batch/obj-${i}.txt"
done

OUT=$("$SHARDEDOBJSTR" list --config "$CONF" --prefix "batch/" 2>/dev/null)
REMAINING=$(echo "$OUT" | grep -c "batch/" || true)
assert_eq "$REMAINING" "5" "5 batch objects remain after partial delete"

# ===================================================================
# PART 19: verify after all operations
# ===================================================================

echo ""
echo "--- verify after writes ---"

OUT=$("$SHARDEDOBJSTR" verify --config "$CONF" 2>&1)
assert_contains "$OUT" "CLEAN" "verify still clean after all ops"

# ===================================================================
# PART 20: --node flag (operate on inner sub-tree)
# ===================================================================

echo ""
echo "--- --node inner ---"

OUT=$("$SHARDEDOBJSTR" list --config "$CONF" --node inner 2>&1)
pass "list with --node inner succeeded"

OUT=$("$SHARDEDOBJSTR" health --config "$CONF" --node inner 2>&1)
assert_contains "$OUT" "HEALTHY" "inner node health is HEALTHY"

OUT=$("$SHARDEDOBJSTR" info --config "$CONF" --node inner 2>&1)
pass "info with --node inner succeeded"

OUT=$("$SHARDEDOBJSTR" verify --config "$CONF" --node inner 2>&1)
assert_contains "$OUT" "CLEAN" "inner node verify is CLEAN"

OUT=$("$SHARDEDOBJSTR" report --config "$CONF" --node inner 2>&1)
pass "report with --node inner succeeded"

OUT=$("$SHARDEDOBJSTR" repair-replication --config "$CONF" --node inner 2>&1)
assert_contains "$OUT" "Repair-replication:" "repair-replication with --node inner runs"

# ===================================================================
# PART 21: format + add-shard + remove-shard (flat raw shards)
# ===================================================================

echo ""
echo "--- add-shard / remove-shard ---"

# Set up 4 raw shards for add/remove tests
SHARD_SIZE=$((64 * 1024 * 1024))
for img in "$IMG_A" "$IMG_B" "$IMG_C" "$IMG_D"; do
    rm -f "$img"
    "$RAWOBJSTR" format --file "$img" --size "$SHARD_SIZE" 2>/dev/null
done

# Put some data into the 4-shard cluster
for i in $(seq 1 8); do
    echo "shard-test-$i" > /tmp/tree_cli_shard_${i}.tmp
    "$SHARDEDOBJSTR" put --shards "$IMG_A,$IMG_B,$IMG_C,$IMG_D" --replicas 2 \
        --key "shard-test/obj-${i}.txt" --from /tmp/tree_cli_shard_${i}.tmp >/dev/null 2>&1
done

# Verify data is there
OUT=$("$SHARDEDOBJSTR" list --shards "$IMG_A,$IMG_B,$IMG_C,$IMG_D" --replicas 2 2>/dev/null)
assert_contains "$OUT" "8 objects" "4-shard cluster has 8 objects"

# add-shard: add a 5th shard
rm -f "$IMG_NEW"
OUT=$("$SHARDEDOBJSTR" add-shard \
    --shards "$IMG_A,$IMG_B,$IMG_C,$IMG_D" --replicas 2 \
    --new-shard "$IMG_NEW" --size "$SHARD_SIZE" 2>&1)
assert_contains "$OUT" "Cluster expanded" "add-shard expands cluster"
assert_contains "$OUT" "(NEW)" "add-shard marks new shard"

# Verify data survives expansion
OUT=$("$SHARDEDOBJSTR" list --shards "$IMG_A,$IMG_B,$IMG_C,$IMG_D,$IMG_NEW" --replicas 2 2>/dev/null)
assert_contains "$OUT" "8 objects" "data survives add-shard"

# remove-shard: drain and remove the new shard
OUT=$("$SHARDEDOBJSTR" remove-shard \
    --shards "$IMG_A,$IMG_B,$IMG_C,$IMG_D,$IMG_NEW" --replicas 2 \
    --remove "$IMG_NEW" 2>&1)
assert_contains "$OUT" "drained successfully" "remove-shard drains shard"
assert_contains "$OUT" "Cluster shrunk" "remove-shard shrinks cluster"

# Verify data survives removal
OUT=$("$SHARDEDOBJSTR" list --shards "$IMG_A,$IMG_B,$IMG_C,$IMG_D" --replicas 2 2>/dev/null)
assert_contains "$OUT" "8 objects" "data survives remove-shard"

# Verify cluster is consistent after add/remove
OUT=$("$SHARDEDOBJSTR" verify --shards "$IMG_A,$IMG_B,$IMG_C,$IMG_D" --replicas 2 2>&1)
assert_contains "$OUT" "CLEAN" "cluster clean after add/remove"

# ===================================================================
# PART 22: repair-replication on flat cluster
# ===================================================================

echo ""
echo "--- repair-replication (flat) ---"

OUT=$("$SHARDEDOBJSTR" repair-replication \
    --shards "$IMG_A,$IMG_B,$IMG_C,$IMG_D" --replicas 2 \
    --batch-size 20 2>&1)
assert_contains "$OUT" "Repair-replication:" "flat repair-replication starts"
assert_contains "$OUT" "Re-replicated:" "flat repair-replication reports results"

# ===================================================================
# PART 23: error handling
# ===================================================================

echo ""
echo "--- error handling ---"

# no args
run_fail "no args prints usage" "$SHARDEDOBJSTR"

# unknown command
run_fail "unknown command fails" "$SHARDEDOBJSTR" nosuchcommand

# put without --key
run_fail "put without --key fails" \
    "$SHARDEDOBJSTR" put --config "$CONF" --from /tmp/tree_cli_put1.tmp

# get without --key
run_fail "get without --key fails" \
    "$SHARDEDOBJSTR" get --config "$CONF" --to /tmp/tree_cli_gone.tmp

# ===================================================================
# done
# ===================================================================

echo ""
echo "=== all shardedobjstr CLI tests complete ==="
