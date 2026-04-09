#!/usr/bin/env python3
"""Empty the demo store and fill it with 200 random files with realistic names."""
import subprocess
import random
import os
import sys

ENDPOINT = "http://127.0.0.1:8000"
BUCKET = "default"

# Realistic directory prefixes and file patterns
prefixes = [
    "reports/2026/q1", "reports/2026/q2", "reports/2025/q4",
    "logs/ingestion", "logs/analytics", "logs/audit",
    "images/products", "images/banners", "images/thumbnails",
    "data/parquet", "data/csv", "data/json",
    "backups/db", "backups/config",
    "models/v3", "models/v4",
    "videos/clips", "videos/previews",
    "documents/invoices", "documents/contracts", "documents/specs",
    "datasets/training", "datasets/validation",
    "exports/customers", "exports/orders",
    "archives/2025", "archives/2024",
]

extensions_by_type = {
    "reports": [".pdf", ".xlsx", ".csv"],
    "logs": [".log", ".jsonl", ".gz"],
    "images": [".jpg", ".png", ".webp"],
    "data": [".parquet", ".csv", ".json"],
    "backups": [".sql", ".tar", ".bak"],
    "models": [".bin", ".onnx", ".pt"],
    "videos": [".mp4", ".webm"],
    "documents": [".pdf", ".docx", ".md"],
    "datasets": [".csv", ".parquet", ".arrow"],
    "exports": [".csv", ".json", ".xlsx"],
    "archives": [".tar.gz", ".zip"],
}

name_parts = [
    "summary", "detail", "report", "snapshot", "export", "dump",
    "intake", "pipeline", "batch", "daily", "weekly", "monthly",
    "final", "draft", "review", "approved", "pending",
    "north", "south", "east", "west", "global", "regional",
    "alpha", "beta", "prod", "staging", "test",
    "server01", "server02", "server03", "gateway", "proxy",
    "product_catalog", "user_events", "transactions", "sessions",
    "hero_banner", "logo", "icon_set", "background",
    "resnet50", "bert_base", "embedding_v2", "classifier",
    "customer_list", "order_history", "inventory", "shipments",
    "contract_renewal", "invoice", "purchase_order", "sla",
    "clip_intro", "preview_hd", "tutorial_part1",
]

def gen_name(i):
    prefix = random.choice(prefixes)
    top = prefix.split("/")[0]
    exts = extensions_by_type.get(top, [".bin"])
    ext = random.choice(exts)
    part1 = random.choice(name_parts)
    part2 = random.choice(name_parts)
    # add a numeric suffix to ensure uniqueness
    num = random.randint(1, 9999)
    name = f"{prefix}/{part1}_{part2}_{num:04d}{ext}"
    return name

# Step 1: List and delete all existing objects
print("=== Emptying store ===")
result = subprocess.run(
    ["curl", "-s", f"{ENDPOINT}/{BUCKET}?list-type=2&max-keys=10000"],
    capture_output=True, text=True
)

# Parse keys from XML
import re
keys = re.findall(r"<Key>([^<]+)</Key>", result.stdout)
print(f"Found {len(keys)} existing objects to delete")

for k in keys:
    subprocess.run(
        ["curl", "-s", "-X", "DELETE", f"{ENDPOINT}/{BUCKET}/{k}"],
        capture_output=True
    )
print(f"Deleted {len(keys)} objects")

# Step 2: Upload 200 random files
print("\n=== Uploading 200 files ===")
names_used = set()
for i in range(200):
    # Generate unique name
    while True:
        name = gen_name(i)
        if name not in names_used:
            names_used.add(name)
            break

    # Random size between 128KB and 10MB
    size = random.randint(128 * 1024, 10 * 1024 * 1024)
    size_kb = size // 1024

    # Generate random data and upload via curl
    subprocess.run(
        ["bash", "-c",
         f"dd if=/dev/urandom bs=1024 count={size_kb} 2>/dev/null | "
         f"curl -s -X PUT --data-binary @- "
         f"'{ENDPOINT}/{BUCKET}/{name}' > /dev/null"],
    )

    if (i + 1) % 20 == 0:
        print(f"  uploaded {i + 1}/200 (last: {name}, {size_kb}KB)")

print("\n=== Done ===")

# Quick summary
result = subprocess.run(
    ["curl", "-s", f"{ENDPOINT}/{BUCKET}?list-type=2&max-keys=10000"],
    capture_output=True, text=True
)
final_keys = re.findall(r"<Key>([^<]+)</Key>", result.stdout)
print(f"Store now has {len(final_keys)} objects")
