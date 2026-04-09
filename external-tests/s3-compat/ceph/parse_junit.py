#!/usr/bin/env python3
"""Parse JUnit XML from parallel ceph test run into a combined results list."""
import xml.etree.ElementTree as ET
import glob
import sys
import os

results_dir = sys.argv[1] if len(sys.argv) > 1 else '/tmp/s3s_ceph_results'

results = {}  # test_name -> status

for xmlfile in sorted(glob.glob(os.path.join(results_dir, 'junit_w*.xml'))):
    tree = ET.parse(xmlfile)
    root = tree.getroot()
    for suite in root.iter('testsuite'):
        for tc in suite.iter('testcase'):
            name = tc.get('name', '')
            if not name:
                continue
            if tc.find('failure') is not None:
                results[name] = 'FAIL'
            elif tc.find('error') is not None:
                results[name] = 'ERROR'
            elif tc.find('skipped') is not None:
                results[name] = 'SKIP'
            else:
                results[name] = 'PASS'

from collections import Counter
counts = Counter(results.values())
print(f"# Total: {len(results)} tests")
print(f"# PASS={counts.get('PASS',0)} FAIL={counts.get('FAIL',0)} ERROR={counts.get('ERROR',0)} SKIP={counts.get('SKIP',0)}")
print()

for name in sorted(results.keys()):
    print(f"{results[name]}\t{name}")
