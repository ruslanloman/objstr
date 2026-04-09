# Example: Auto-Syncing Local Mirror

Run a local objstrd as your development endpoint and keep it automatically
synced with a remote S3 store (AWS, R2, another objstrd, etc.). When either
side goes offline (network loss, USB unplugged), the other keeps working.
When it comes back, `aws s3 sync --delete` reconciles both sides -- no
tombstones or config swapping needed.

This uses two independent objstrd instances so both sides are S3 endpoints
and `aws s3 sync` works natively between them.

## Architecture

```
  [Your app]
      |
      v
  objstrd-local (port 8001)     <-- always running, fast local reads/writes
      |
  sync-mirror.sh                <-- background sync daemon
      |
      v
  AWS S3 / R2 / remote objstrd  <-- cloud copy, survives local disk loss
```

## Step 1: Start the Local objstrd

```bash
# Local store on a raw image (or fs, or a USB-mounted raw device)
objstrd --image /home/dev/local-store.raw --size-mb 2048 \
  --port 8001 --bucket project-data
```

Your app talks to `http://localhost:8001` as its S3 endpoint. This works
whether you are online or offline.

## Step 2: Configure the Sync

Create `sync-mirror.conf`:

```bash
# Local objstrd (always available)
LOCAL_ENDPOINT=http://localhost:8001
LOCAL_BUCKET=project-data

# Remote (AWS S3)
REMOTE_ENDPOINT=https://s3.us-east-1.amazonaws.com
REMOTE_BUCKET=my-project-data
REMOTE_REGION=us-east-1

# Sync every 30 seconds, mirror deletes
SYNC_INTERVAL=30
SYNC_DELETE=true
SYNC_DIRECTION=bidirectional
```

For Cloudflare R2:

```bash
REMOTE_ENDPOINT=https://ACCOUNT_ID.r2.cloudflarestorage.com
REMOTE_BUCKET=my-project-data
REMOTE_REGION=auto
```

For a second objstrd (e.g. team server):

```bash
REMOTE_ENDPOINT=http://10.0.1.50:8000
REMOTE_BUCKET=project-data
REMOTE_REGION=us-east-1
```

## Step 3: Start the Sync Daemon

```bash
# Set AWS credentials for the remote endpoint
export AWS_ACCESS_KEY_ID=AKIA...
export AWS_SECRET_ACCESS_KEY=wJal...

# Start the sync loop
./external-tests/sync-mirror/sync-mirror.sh sync-mirror.conf
```

The script checks both endpoints every `SYNC_INTERVAL` seconds:

- **Both online:** runs `aws s3 sync` in the configured direction
- **Remote goes offline:** logs it, local keeps serving, no errors
- **Remote comes back:** automatically pushes local changes to remote
- **Local goes offline** (USB unplugged): logs it, remote still has data
- **Local comes back** (USB plugged in): pulls remote changes to local

## Step 4: Seed Initial Data (first time only)

```bash
# If the remote already has data, pull it to local
aws s3 sync s3://my-project-data/ s3://project-data/ \
  --endpoint-url http://localhost:8001

# Or if local has the data, push it to remote
aws s3 sync s3://project-data/ s3://my-project-data/ \
  --source-region us-east-1 \
  --endpoint-url http://localhost:8001
```

## Example: USB-Mounted Raw Store

A developer keeps a raw store on a USB stick for portability:

```bash
# Format the USB device once
rawobjstr format --image /mnt/usb/portable.raw --size-mb 4096

# Start objstrd on the USB store
objstrd --image /mnt/usb/portable.raw --port 8001 --bucket data

# Start sync to keep AWS in sync
./sync-mirror.sh sync-mirror.conf &

# Work normally
aws s3 cp report.pdf s3://data/reports/q1.pdf --endpoint-url http://localhost:8001

# Unplug USB -- sync script notices local went offline
# AWS still has all the data from the last sync

# Plug USB back in, restart objstrd on it
objstrd --image /mnt/usb/portable.raw --port 8001 --bucket data
# sync-mirror.sh notices local is back, pulls any remote changes
```

## Design Notes

- **Conflict resolution is last-sync-wins.** If the same key is written on
  both sides between syncs, `aws s3 sync` picks the newer one by timestamp.
  For teams, use prefix partitioning (e.g. `dev-alice/`, `dev-bob/`) to
  avoid conflicts.
- **Sync is eventually consistent.** There is a window of up to
  `SYNC_INTERVAL` seconds where the two sides may differ.
- **The sync script stages through a temp directory** because `aws s3 sync`
  does not support two different `--endpoint-url` values in a single
  command. For large stores, consider using rclone which supports two
  different S3 remotes directly.
- **Deletes are handled by `--delete`.** Set `SYNC_DELETE=false` if you
  want append-only behavior (never delete from remote).

See `external-tests/sync-mirror/` for the script and example config.
