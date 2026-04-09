#!/usr/bin/env python3
"""
parse_mint_results.py - Parse mint JSON log into human-readable summary

Reads the JSON log file (one JSON object per line) produced by s3_mint_tests.py
and outputs a categorized summary suitable for inclusion in mint-tests.md.

Usage:
    python3 parse_mint_results.py [log_file]
    python3 parse_mint_results.py /tmp/mint_results/log.json

Output format (tab-separated, one line per test):
    STATUS\ttest_name\tduration_ms\talert_message
"""

import json
import sys
import os


def parse_log(log_path):
    """Parse the JSON log file and return list of test results."""
    results = []
    with open(log_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
                results.append(entry)
            except json.JSONDecodeError:
                continue
    return results


def categorize_tests(results):
    """Group tests by category based on function name patterns."""
    categories = {
        "Bucket Operations": [],
        "Put/Get/Head/Delete": [],
        "Metadata & Content-Type": [],
        "Copy Object": [],
        "Range Reads": [],
        "Listing (V1 & V2)": [],
        "Multipart Upload": [],
        "Batch Delete": [],
        "Special Characters": [],
        "Overwrite": [],
        "Transfer Manager": [],
        "ETag": [],
        "Other": [],
    }

    category_map = {
        "bucket": "Bucket Operations",
        "head_bucket": "Bucket Operations",
        "list_buckets": "Bucket Operations",
        "make_bucket": "Bucket Operations",
        "delete_bucket": "Bucket Operations",
        "get_bucket": "Bucket Operations",
        "put_object": "Put/Get/Head/Delete",
        "get_object": "Put/Get/Head/Delete",
        "head_object": "Put/Get/Head/Delete",
        "delete_object": "Put/Get/Head/Delete",
        "metadata": "Metadata & Content-Type",
        "content_type": "Metadata & Content-Type",
        "copy_object": "Copy Object",
        "range": "Range Reads",
        "list_objects": "Listing (V1 & V2)",
        "list_object": "Listing (V1 & V2)",
        "multipart": "Multipart Upload",
        "list_parts": "Multipart Upload",
        "list_multipart": "Multipart Upload",
        "delete_objects": "Batch Delete",
        "special": "Special Characters",
        "deep_path": "Special Characters",
        "dots_in_key": "Special Characters",
        "plus_in_key": "Special Characters",
        "overwrite": "Overwrite",
        "transfer_manager": "Transfer Manager",
        "etag": "ETag",
    }

    for result in results:
        func = result.get("function", "")
        placed = False
        for pattern, cat in category_map.items():
            if pattern in func:
                categories[cat].append(result)
                placed = True
                break
        if not placed:
            categories["Other"].append(result)

    return categories


def print_results(results, format_type="summary"):
    """Print results in the requested format."""
    categories = categorize_tests(results)

    total_pass = sum(1 for r in results if r["status"] == "PASS")
    total_fail = sum(1 for r in results if r["status"] == "FAIL")
    total_na = sum(1 for r in results if r["status"] == "NA")
    total = len(results)

    if format_type == "tsv":
        # Tab-separated output for scripting
        for r in results:
            alert = r.get("alert", "")
            print("{}\t{}\t{}\t{}".format(
                r["status"], r["function"], r["duration"], alert))
        return

    # Human-readable summary
    print("# Mint Test Results")
    print()
    print("Total: {} passed, {} failed, {} skipped out of {} tests".format(
        total_pass, total_fail, total_na, total))
    print()

    for cat_name, cat_results in categories.items():
        if not cat_results:
            continue

        cat_pass = sum(1 for r in cat_results if r["status"] == "PASS")
        cat_fail = sum(1 for r in cat_results if r["status"] == "FAIL")
        cat_na = sum(1 for r in cat_results if r["status"] == "NA")

        print("## {} ({}/{} passed)".format(cat_name, cat_pass, len(cat_results)))
        print()
        print("| Test | Status | Duration (ms) | Notes |")
        print("|------|--------|---------------|-------|")

        for r in cat_results:
            status = r["status"]
            if status == "PASS":
                symbol = "PASS"
            elif status == "FAIL":
                symbol = "FAIL"
            elif status == "NA":
                symbol = "SKIP"
            else:
                symbol = status

            alert = r.get("alert", r.get("message", ""))
            if len(alert) > 80:
                alert = alert[:77] + "..."

            print("| `{}` | {} | {} | {} |".format(
                r["function"], symbol, r["duration"], alert))
        print()

    # Print failures detail
    failures = [r for r in results if r["status"] == "FAIL"]
    if failures:
        print("## Failure Details")
        print()
        for r in failures:
            print("### `{}`".format(r["function"]))
            if r.get("alert"):
                print("Alert: {}".format(r["alert"]))
            if r.get("error"):
                print("```")
                print(r["error"][:500])
                print("```")
            print()


def main():
    if len(sys.argv) < 2:
        log_path = "/tmp/mint_results/log.json"
    else:
        log_path = sys.argv[1]

    if not os.path.exists(log_path):
        print("ERROR: Log file not found: {}".format(log_path))
        sys.exit(1)

    format_type = "summary"
    if "--tsv" in sys.argv:
        format_type = "tsv"

    results = parse_log(log_path)
    if not results:
        print("ERROR: No test results found in {}".format(log_path))
        sys.exit(1)

    print_results(results, format_type)


if __name__ == "__main__":
    main()
