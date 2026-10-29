"""Validate recorded artifacts and produce exact, non-aggregated gate summaries."""

import collections
import json
import xml.etree.ElementTree as ET
from pathlib import Path

OUT = Path(__file__).resolve().parent
summary = {}
for path in sorted(OUT.glob("*.xml")):
    root = ET.parse(path).getroot()
    cases = list(root.iter("testcase"))
    summary[path.name] = {
        "tests": len(cases),
        "passed": sum(
            c.find("failure") is None and c.find("error") is None and c.find("skipped") is None for c in cases
        ),
        "failed": [
            {"name": c.attrib.get("name"), "message": c.find("failure").attrib.get("message", "")}
            for c in cases
            if c.find("failure") is not None
        ],
        "errors": [c.attrib.get("name") for c in cases if c.find("error") is not None],
        "skipped": sum(c.find("skipped") is not None for c in cases),
    }
(OUT / "test_results_summary.json").write_text(json.dumps(summary, indent=2))
rows = json.loads((OUT / "equivalence_pages.json").read_text())
groups = collections.defaultdict(list)
for row in rows:
    assert row["old_keys"] == row["new_keys"]
    groups[(row["limit"], json.dumps(row["filters"], sort_keys=True))].append(row)
traversals = []
for (limit, filters), pages in groups.items():
    assert pages[0]["after"] is None
    assert not pages[-1]["has_more"]
    assert all(p["has_more"] for p in pages[:-1])
    assert all(p["upper"] == pages[0]["upper"] for p in pages)
    keys = [key for page in pages for key in page["old_keys"]]
    assert keys == sorted(keys)
    for previous, current in zip(pages, pages[1:]):
        assert current["after"] == dict(zip(("created_at", "memory_type", "id"), previous["old_keys"][-1]))
    if filters == "{}":
        assert len(keys) == 12000
    traversals.append(
        {
            "limit": limit,
            "filters": json.loads(filters),
            "pages": len(pages),
            "records": len(keys),
            "first": keys[0] if keys else None,
            "last": keys[-1] if keys else None,
            "upper": pages[0]["upper"],
        }
    )
(OUT / "equivalence_summary.json").write_text(
    json.dumps(
        {
            "reference": "unmodified ad50ec3 SQLiteProvider",
            "compared_pages": len(rows),
            "traversals": traversals,
            "verified": "every triple, has_more, fixed upper and inter-page after boundary",
        },
        indent=2,
    )
)
plans = json.loads((OUT / "eqp.json").read_text())
assert len(plans) == 200
counts = collections.Counter()
lifecycle = []
for row in plans:
    joined = "\n".join(row["eqp"])
    if row["version"] == "after" and not row["filters"].get("lifecycle_states") and not row["filters"].get("valid_at"):
        assert "TEMP B-TREE" not in joined, row
        for kind in row["kinds"]:
            assert f"USING INDEX ix_{kind}s_created_type_id" in joined, row
        counts["after_no_lifecycle_or_valid_at_indexed_no_temp"] += 1
    if row["version"] == "after" and row["filters"].get("lifecycle_states"):
        if "belief" in row["kinds"]:
            assert "USING INDEX ix_beliefs_lifecycle_state" in joined
            assert "TEMP B-TREE" in joined
        lifecycle.append({k: row[k] for k in ("limit", "kinds", "role", "eqp")})
(OUT / "eqp_summary.json").write_text(
    json.dumps({"statements": len(plans), "verified": dict(counts), "lifecycle_actual_plans": lifecycle}, indent=2)
)
print(
    "gateway_final:", summary["gateway_final.xml"]["tests"], "tests;", summary["gateway_final.xml"]["passed"], "passed"
)
print("equivalence pages:", len(rows))
print("EQP verified:", dict(counts))
print("all recorded JUnit runs:", len(summary))
