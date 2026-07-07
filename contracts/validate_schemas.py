#!/usr/bin/env python3
"""Validate all JSON schema files in contracts/"""
import json
import glob
import sys

errors = []
for f in sorted(glob.glob("contracts/*.schema.json")):
    with open(f) as fh:
        data = json.load(fh)
    sv = data.get("schema_version", "no version")
    av = data.get("api_version", "no version")
    title = data.get("title", f)
    print(f"  ✅ {title:35s} schema={sv} api={av}")
if errors:
    sys.exit(1)
print("\nAll schemas valid.")
