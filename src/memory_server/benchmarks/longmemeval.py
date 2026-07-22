"""LongMemEval-S lineage-aware benchmark harness.

The harness keeps retrieval fixed and recomputes metrics against three
scoring targets:

* raw: exact raw source/evidence turn memories only;
* source: raw source memories plus all descendants sharing the source anchor;
* canonical: current serving memories derived from the source anchor.

This follows the TIAP-style evaluation contract described in the CMMS v0.11
research notes: provenance first, then target-specific rescoring.
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import asdict, dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


class Target(StrEnum):
    """Supported retrieval scoring targets."""

    RAW = "raw"
    SOURCE = "source"
    CANONICAL = "canonical"


TOKEN_RE = re.compile(r"[\w']+", re.UNICODE)


@dataclass(frozen=True)
class BenchmarkQuery:
    """A LongMemEval query plus turn/session-level retrieval fixture."""

    query_id: str
    question: str
    answer: str
    question_type: str
    source_session_ids: tuple[str, ...] = ()
    source_turn_ids: tuple[str, ...] = ()
    question_date: str | None = None
    is_abstention: bool = False
    sessions: tuple[tuple[dict[str, Any], ...], ...] = ()
    session_ids: tuple[str, ...] = ()
    session_dates: tuple[str, ...] = ()

    @property
    def has_retrieval_fixture(self) -> bool:
        """Whether this query can be included in retrieval metrics."""

        return bool(self.source_session_ids or self.source_turn_ids)


@dataclass(frozen=True)
class MemoryItem:
    """Lineage contract for one stored memory item.

    Non-raw items must carry source_turn_ids/source_session_ids so the harness
    can map derived memories back to LongMemEval evidence anchors.
    """

    memory_id: str
    kind: str
    content: str
    source_turn_ids: tuple[str, ...]
    source_session_ids: tuple[str, ...]
    derived_from_memory_ids: tuple[str, ...] = ()
    raw: bool = False
    canonical: bool = False
    valid_from: str | None = None
    valid_to: str | None = None
    currentness: str = "timeless"
    entity_keys: tuple[str, ...] = ()
    attribute_keys: tuple[str, ...] = ()
    update_group_id: str | None = None
    confidence: float | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.memory_id:
            raise ValueError("memory_id must be non-empty")
        if not self.kind:
            raise ValueError("kind must be non-empty")
        if not self.raw and not (self.source_turn_ids or self.source_session_ids):
            raise ValueError("non-raw memory items must have a source anchor")
        if self.raw and self.canonical:
            raise ValueError("canonical items must be non-raw")


@dataclass(frozen=True)
class RetrievedItem:
    """One retrieved memory item in a saved trace."""

    rank: int
    memory_id: str
    score: float


@dataclass(frozen=True)
class RetrievalTrace:
    """Fixed retrieval output for a query, saved independently of scoring."""

    query_id: str
    benchmark: str
    retriever: str
    top_k: tuple[RetrievedItem, ...]
    store_snapshot_id: str | None = None
    timestamp: str | None = None

    @property
    def ranked_ids(self) -> list[str]:
        """Memory ids ordered by rank."""

        return [item.memory_id for item in sorted(self.top_k, key=lambda item: item.rank)]


@dataclass(frozen=True)
class RetrievalTargetScore:
    """Retrieval metrics for one target on one query."""

    target: Target
    eligible_ids: frozenset[str]
    hit_at: dict[int, int]
    recall_at: dict[int, float]
    ndcg_at: dict[int, float]


class LongMemEvalLoader:
    """Loader and fixture builder for LongMemEval JSON files."""

    @classmethod
    def load_json(cls, path: str | Path, *, limit: int | None = None) -> list[BenchmarkQuery]:
        """Load a LongMemEval JSON array from disk."""

        records = json.loads(Path(path).read_text())
        if not isinstance(records, list):
            raise ValueError("LongMemEval file must contain a JSON array")
        if limit is not None:
            records = records[:limit]
        return [cls.query_from_record(record) for record in records]

    @staticmethod
    def query_from_record(record: Mapping[str, Any]) -> BenchmarkQuery:
        """Convert a LongMemEval record into a BenchmarkQuery.

        Turn IDs are deterministic and stable within the harness:
        ``{question_id}:{session_id}:{turn_index}``.
        """

        query_id = str(record["question_id"])
        question_type = str(record.get("question_type", ""))
        session_ids = tuple(str(sid) for sid in record.get("haystack_session_ids", ()))
        sessions = tuple(
            tuple(dict(turn) for turn in session)
            for session in record.get("haystack_sessions", ())
        )
        answer_session_ids = tuple(str(sid) for sid in record.get("answer_session_ids", ()))
        answer_session_set = set(answer_session_ids)
        source_turn_ids: list[str] = []

        for session_idx, session in enumerate(sessions):
            session_id = session_ids[session_idx] if session_idx < len(session_ids) else str(session_idx)
            if session_id not in answer_session_set:
                continue
            for turn_idx, turn in enumerate(session):
                if turn.get("has_answer") is True:
                    source_turn_ids.append(_turn_id(query_id, session_id, turn_idx))

        is_abstention = query_id.endswith("_abs")
        return BenchmarkQuery(
            query_id=query_id,
            question=str(record.get("question", "")),
            answer=str(record.get("answer", "")),
            question_type=question_type,
            source_session_ids=answer_session_ids,
            source_turn_ids=tuple(source_turn_ids),
            question_date=record.get("question_date"),
            is_abstention=is_abstention,
            sessions=sessions,
            session_ids=session_ids,
            session_dates=tuple(str(date) for date in record.get("haystack_dates", ())),
        )


def build_target_sets(query: BenchmarkQuery, memory_items: Iterable[MemoryItem]) -> dict[Target, set[str]]:
    """Build raw/source/canonical eligible memory ID sets for one query."""

    source_turns = set(query.source_turn_ids)
    source_sessions = set(query.source_session_ids)
    targets: dict[Target, set[str]] = {target: set() for target in Target}

    for item in memory_items:
        if not _shares_source_anchor(item, source_turns, source_sessions):
            continue
        if item.raw:
            targets[Target.RAW].add(item.memory_id)
            targets[Target.SOURCE].add(item.memory_id)
            continue

        targets[Target.SOURCE].add(item.memory_id)
        if item.canonical and item.currentness != "historical":
            targets[Target.CANONICAL].add(item.memory_id)

    return targets


def recall_at_k(ranked_ids: Sequence[str], relevant_ids: set[str] | frozenset[str], k: int) -> float:
    """Compute recall@k for a ranked ID list."""

    if not relevant_ids:
        return 0.0
    top = set(ranked_ids[:k])
    return len(top & set(relevant_ids)) / len(relevant_ids)


def ndcg_at_k(ranked_ids: Sequence[str], relevant_ids: set[str] | frozenset[str], k: int) -> float:
    """Compute binary-relevance nDCG@k."""

    if not relevant_ids:
        return 0.0
    dcg = 0.0
    for idx, memory_id in enumerate(ranked_ids[:k], start=1):
        if memory_id in relevant_ids:
            dcg += 1.0 / math.log2(idx + 1)
    ideal_hits = min(len(relevant_ids), k)
    idcg = sum(1.0 / math.log2(idx + 1) for idx in range(1, ideal_hits + 1))
    if idcg == 0:
        return 0.0
    return dcg / idcg


def rescore_trace(
    query: BenchmarkQuery,
    memory_items: Iterable[MemoryItem],
    trace: RetrievalTrace,
    *,
    ks: Sequence[int] = (5, 10),
) -> dict[Target, RetrievalTargetScore]:
    """Rescore a saved retrieval trace against every target."""

    target_sets = build_target_sets(query, memory_items)
    ranked_ids = trace.ranked_ids
    scores: dict[Target, RetrievalTargetScore] = {}

    for target, eligible_ids in target_sets.items():
        frozen = frozenset(eligible_ids)
        scores[target] = RetrievalTargetScore(
            target=target,
            eligible_ids=frozen,
            hit_at={k: int(recall_at_k(ranked_ids, frozen, k) > 0.0) for k in ks},
            recall_at={k: recall_at_k(ranked_ids, frozen, k) for k in ks},
            ndcg_at={k: ndcg_at_k(ranked_ids, frozen, k) for k in ks},
        )

    return scores


def compare_targets_on_shared_subset(
    target_sets_by_query: Mapping[str, Mapping[Target, set[str]]],
    traces_by_query: Mapping[str, RetrievalTrace],
    *,
    left: Target,
    right: Target,
    k: int,
) -> dict[str, float | int | str]:
    """Compare two targets only on queries covered by both target sets."""

    left_recalls: list[float] = []
    right_recalls: list[float] = []
    changed = 0

    for query_id, target_sets in target_sets_by_query.items():
        left_ids = target_sets.get(left, set())
        right_ids = target_sets.get(right, set())
        trace = traces_by_query.get(query_id)
        if not trace or not left_ids or not right_ids:
            continue
        ranked_ids = trace.ranked_ids
        left_recall = recall_at_k(ranked_ids, left_ids, k)
        right_recall = recall_at_k(ranked_ids, right_ids, k)
        left_recalls.append(left_recall)
        right_recalls.append(right_recall)
        if left_recall != right_recall:
            changed += 1

    query_count = len(left_recalls)
    return {
        "left": left.value,
        "right": right.value,
        "k": k,
        "query_count": query_count,
        "left_recall_at_k": _mean(left_recalls),
        "right_recall_at_k": _mean(right_recalls),
        "delta_recall_at_k": _mean(right_recalls) - _mean(left_recalls),
        "changed_queries": changed,
        "change_rate": changed / query_count if query_count else 0.0,
    }


def run_builtin_baseline(
    dataset_path: str | Path,
    *,
    output_path: str | Path,
    top_k: int = 10,
    limit: int | None = None,
) -> dict[str, Any]:
    """Run the deterministic Hermes built-in baseline over LongMemEval-S.

    Writes one JSONL row per query with the fixed retrieval trace and all
    target-specific scores. Returns an aggregate summary suitable for CLI
    output or a benchmark report.
    """

    queries = LongMemEvalLoader.load_json(dataset_path, limit=limit)
    baseline = BuiltInMemoryBaseline()
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    target_sets_by_query: dict[str, dict[Target, set[str]]] = {}
    traces_by_query: dict[str, RetrievalTrace] = {}
    aggregate: dict[Target, dict[str, list[float]]] = {
        target: {"recall": [], "ndcg": [], "hit": []}
        for target in Target
    }
    retrieval_fixture_count = 0

    with output.open("w") as handle:
        for query in queries:
            memory_items = baseline.ingest(query)
            trace = baseline.retrieve(query, top_k=top_k)
            target_sets = build_target_sets(query, memory_items)
            target_sets_by_query[query.query_id] = target_sets
            traces_by_query[query.query_id] = trace

            scores = rescore_trace(query, memory_items, trace, ks=(top_k,))
            if query.has_retrieval_fixture:
                retrieval_fixture_count += 1
                for target, score in scores.items():
                    if not score.eligible_ids:
                        continue
                    aggregate[target]["recall"].append(score.recall_at[top_k])
                    aggregate[target]["ndcg"].append(score.ndcg_at[top_k])
                    aggregate[target]["hit"].append(float(score.hit_at[top_k]))

            row = {
                "query_id": query.query_id,
                "question_type": query.question_type,
                "is_abstention": query.is_abstention,
                "trace": _trace_to_dict(trace),
                "target_sets": {target.value: sorted(ids) for target, ids in target_sets.items()},
                "scores": {target.value: _score_to_dict(score) for target, score in scores.items()},
            }
            handle.write(json.dumps(row, sort_keys=True) + "\n")

    metrics = {
        target.value: {
            f"hit@{top_k}": _mean(values["hit"]),
            f"recall@{top_k}": _mean(values["recall"]),
            f"ndcg@{top_k}": _mean(values["ndcg"]),
            "covered_queries": len(values["recall"]),
        }
        for target, values in aggregate.items()
    }

    return {
        "benchmark": "longmemeval_s",
        "retriever": baseline.retriever_name,
        "dataset_path": str(dataset_path),
        "output_path": str(output),
        "query_count": len(queries),
        "retrieval_fixture_count": retrieval_fixture_count,
        "top_k": top_k,
        "metrics": metrics,
        "shared_subset": {
            "raw_vs_source": compare_targets_on_shared_subset(
                target_sets_by_query,
                traces_by_query,
                left=Target.RAW,
                right=Target.SOURCE,
                k=top_k,
            ),
            "raw_vs_canonical": compare_targets_on_shared_subset(
                target_sets_by_query,
                traces_by_query,
                left=Target.RAW,
                right=Target.CANONICAL,
                k=top_k,
            ),
        },
    }


class BuiltInMemoryBaseline:
    """Deterministic Hermes built-in baseline approximation.

    Hermes built-in memory is a raw-note store without CMMS lineage transforms.
    This baseline therefore ingests LongMemEval sessions as raw turn memories and
    retrieves them with a transparent lexical overlap scorer. It gives the
    harness a stable baseline path that requires no API keys or model calls.
    """

    retriever_name = "hermes_builtin_lexical"

    def __init__(self) -> None:
        self._items_by_query: dict[str, list[MemoryItem]] = {}

    def ingest(self, query: BenchmarkQuery) -> list[MemoryItem]:
        """Ingest all query sessions as raw turn MemoryItems."""

        items: list[MemoryItem] = []
        for session_idx, session in enumerate(query.sessions):
            session_id = query.session_ids[session_idx] if session_idx < len(query.session_ids) else str(session_idx)
            for turn_idx, turn in enumerate(session):
                memory_id = _turn_id(query.query_id, session_id, turn_idx)
                items.append(
                    MemoryItem(
                        memory_id=memory_id,
                        kind="raw_turn",
                        content=str(turn.get("content", "")),
                        source_turn_ids=(memory_id,),
                        source_session_ids=(session_id,),
                        raw=True,
                        canonical=False,
                        metadata={"role": turn.get("role"), "has_answer": turn.get("has_answer", False)},
                    )
                )
        self._items_by_query[query.query_id] = items
        return items

    def retrieve(self, query: BenchmarkQuery, *, top_k: int = 10) -> RetrievalTrace:
        """Retrieve top-k raw memories for a query."""

        items = self._items_by_query.get(query.query_id)
        if items is None:
            items = self.ingest(query)
        query_tokens = _tokens(query.question)
        scored = [
            (idx, _lexical_score(query_tokens, item.content), item)
            for idx, item in enumerate(items)
        ]
        ranked = sorted(scored, key=lambda row: (-row[1], row[0]))[:top_k]
        retrieved = tuple(
            RetrievedItem(rank=rank, memory_id=item.memory_id, score=score)
            for rank, (_idx, score, item) in enumerate(ranked, start=1)
        )
        return RetrievalTrace(
            query_id=query.query_id,
            benchmark="longmemeval_s",
            retriever=self.retriever_name,
            top_k=retrieved,
        )


def _turn_id(query_id: str, session_id: str, turn_idx: int) -> str:
    return f"{query_id}:{session_id}:{turn_idx}"


def _shares_source_anchor(item: MemoryItem, source_turns: set[str], source_sessions: set[str]) -> bool:
    if source_turns and set(item.source_turn_ids) & source_turns:
        return True
    return bool(source_sessions and set(item.source_session_ids) & source_sessions)


def _tokens(text: str) -> set[str]:
    return {token.lower() for token in TOKEN_RE.findall(text)}


def _lexical_score(query_tokens: set[str], text: str) -> float:
    if not query_tokens:
        return 0.0
    doc_tokens = _tokens(text)
    if not doc_tokens:
        return 0.0
    overlap = len(query_tokens & doc_tokens)
    return overlap / math.sqrt(len(query_tokens) * len(doc_tokens))


def _mean(values: Sequence[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _trace_to_dict(trace: RetrievalTrace) -> dict[str, Any]:
    data = asdict(trace)
    data["top_k"] = [asdict(item) for item in trace.top_k]
    return data


def _score_to_dict(score: RetrievalTargetScore) -> dict[str, Any]:
    return {
        "target": score.target.value,
        "eligible_ids": sorted(score.eligible_ids),
        "hit_at": score.hit_at,
        "recall_at": score.recall_at,
        "ndcg_at": score.ndcg_at,
    }
