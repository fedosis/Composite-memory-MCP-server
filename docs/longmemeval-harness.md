# LongMemEval-S Benchmark Harness (v0.11)

CMMS v0.11 adds a lineage-aware evaluation harness for LongMemEval-S. The goal is to avoid a single ambiguous `Recall@k` number when the memory store contains raw turns, source-linked derived memories, and canonical/serving memories.

## Inputs

1. LongMemEval-S JSON file (`longmemeval_s_cleaned.json` from the official cleaned dataset).
2. Memory items following the harness lineage contract:
   - `memory_id`
   - `kind`: `raw_turn`, `belief`, `canonical_fact`, `summary`, or `state_view`
   - `content`
   - `source_turn_ids`
   - `source_session_ids`
   - `derived_from_memory_ids`
   - `raw`
   - `canonical`
   - `valid_from` / `valid_to`
   - `currentness`: `current`, `historical`, or `timeless`
   - optional `entity_keys`, `attribute_keys`, `update_group_id`, `confidence`
3. Saved retrieval traces: fixed ranked `memory_id` outputs per query.

## Scoring targets

The harness computes the same fixed retrieval trace against three targets:

- `raw`: exact raw source/evidence turn memories only.
- `source`: raw source memories plus descendants sharing the source anchor.
- `canonical`: current non-raw serving memories derived from the source anchor.

Historical canonical items are excluded from canonical credit for all query types.

## Metrics

For each target:

- `hit@k`
- `recall@k`
- `ndcg@k`
- coverage count

Pairwise comparisons use shared-subset policy: a query is included only when both compared targets have at least one eligible memory ID.

## Built-in Hermes baseline

The first baseline is deterministic and model-free:

```bash
memory-server benchmark-longmemeval data/longmemeval_s_cleaned.json \
  --output benchmark-results/longmemeval_builtin.jsonl \
  --top-k 10
```

It ingests each LongMemEval history turn as a raw memory item and retrieves with transparent lexical overlap. This approximates Hermes built-in raw-memory behavior and provides a stable baseline with no API key requirement.

The JSONL output stores one row per query:

- query metadata
- fixed retrieval trace
- raw/source/canonical target sets
- per-target scores

The CLI prints an aggregate JSON summary for reports and CI logs.

## Interpretation rules

- Do not publish a retrieval score without its target label.
- Treat `source` as diagnostic coverage, not a sole headline score.
- Use `raw` as evidence-retention anchor.
- Use `canonical` as serving-memory signal only with semantic audit on contested hits in later phases.
- Keep answer accuracy, abstention accuracy, and stale-state diagnostics separate from retrieval metrics.
