#!/usr/bin/env python3
"""Update ceph-tests.md with a new run column from parse_junit results."""
import re
import sys

EMOJI = {"PASS": "✅", "FAIL": "❌", "ERROR": "⚠️", "SKIP": "🚫"}

def load_results(path):
    """Load test results from parse_junit output file."""
    results = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(None, 1)
            if len(parts) == 2:
                status, name = parts
                results[name] = status
    return results

def update_md(md_path, results, run_label):
    """Add a new column to every table in ceph-tests.md."""
    with open(md_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    new_lines = []
    i = 0
    while i < len(lines):
        line = lines[i]

        # Update the Overall Summary table
        if line.startswith("| Run ") and "Passed" in line:
            # Header row - add nothing here, we'll add a new data row after the table
            new_lines.append(line)
            i += 1
            continue

        # Detect table header rows with "Run 1" (or last run) - add new run column
        if re.match(r'\| Test\s+\|.*\| Run \d+', line):
            # Add new column header
            line = line.rstrip("\n") + f" {run_label} |\n"
            new_lines.append(line)
            i += 1
            # Next line is separator
            if i < len(lines) and lines[i].startswith("|---"):
                sep = lines[i].rstrip("\n") + "-------|" + "\n"
                new_lines.append(sep)
                i += 1
            continue

        # Detect table data rows with test names
        m = re.match(r'\| `(test_\w+)` \|', line)
        if m:
            test_name = m.group(1)
            status = results.get(test_name, "—")
            emoji = EMOJI.get(status, "—")
            line = line.rstrip("\n") + f" {emoji} |\n"
            new_lines.append(line)
            i += 1
            continue

        new_lines.append(line)
        i += 1

    # Add new row to Overall Summary table
    # Count results
    pass_count = sum(1 for v in results.values() if v == "PASS")
    fail_count = sum(1 for v in results.values() if v == "FAIL")
    error_count = sum(1 for v in results.values() if v == "ERROR")
    skip_count = sum(1 for v in results.values() if v == "SKIP")
    total = len(results)

    summary_row = f"| {run_label} | {sys.argv[4] if len(sys.argv) > 4 else 'TBD'} | **{pass_count}** | **{fail_count}** | **{error_count}** | **{skip_count}** | {total} | Path encoding fix |\n"

    # Find where to insert summary row (after last data row in Overall Summary)
    final_lines = []
    inserted_summary = False
    for j, line in enumerate(new_lines):
        final_lines.append(line)
        # Look for the last row of the summary table (starts with "| Run")
        if not inserted_summary and line.startswith("| Run ") and "829" in line:
            # Check if next line is blank or --- (end of table)
            if j + 1 < len(new_lines) and (new_lines[j+1].strip() == "" or new_lines[j+1].startswith("---")):
                final_lines.append(summary_row)
                inserted_summary = True

    with open(md_path, "w", encoding="utf-8") as f:
        f.writelines(final_lines)

    print(f"Updated {md_path} with {run_label}: {pass_count}P/{fail_count}F/{error_count}E/{skip_count}S")

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print(f"Usage: {sys.argv[0]} <ceph-tests.md> <results.txt> <RunLabel> [date]")
        sys.exit(1)
    results = load_results(sys.argv[2])
    update_md(sys.argv[1], results, sys.argv[3])
