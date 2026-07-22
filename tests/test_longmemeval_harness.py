"""Tests for the LongMemEval-S lineage-aware benchmark harness."""

from typer.testing import CliRunner

from memory_server.benchmarks.longmemeval import (
    BenchmarkQuery,
    BuiltInMemoryBaseline,
    LongMemEvalLoader,
    MemoryItem,
    RetrievalTrace,
    RetrievedItem,
    Target,
    build_target_sets,
    compare_targets_on_shared_subset,
    ndcg_at_k,
    recall_at_k,
    rescore_trace,
    run_builtin_baseline,
)
from memory_server.cli import app

runner = CliRunner()


def _sample_record():
    return {
        "question_id": "q1",
        "question_type": "knowledge-update",
        "question": "Where does Alex live now?",
        "answer": "Berlin",
        "question_date": "2026-01-04",
        "haystack_session_ids": ["s_old", "s_new"],
        "haystack_dates": ["2026-01-01", "2026-01-03"],
        "answer_session_ids": ["s_new"],
        "haystack_sessions": [
            [
                {"role": "user", "content": "Alex moved to Paris."},
                {"role": "assistant", "content": "Noted."},
            ],
            [
                {"role": "user", "content": "Alex moved from Paris to Berlin.", "has_answer": True},
                {"role": "assistant", "content": "I will remember Berlin.", "has_answer": True},
            ],
        ],
    }


def test_loader_builds_turn_level_source_anchors_from_longmemeval_record():
    query = LongMemEvalLoader.query_from_record(_sample_record())

    assert query.query_id == "q1"
    assert query.question_type == "knowledge-update"
    assert query.is_abstention is False
    assert query.source_session_ids == ("s_new",)
    assert query.source_turn_ids == ("q1:s_new:0", "q1:s_new:1")


def test_loader_marks_abs_suffix_as_abstention_without_retrieval_fixture():
    record = _sample_record() | {
        "question_id": "q2_abs",
        "answer_session_ids": [],
        "haystack_sessions": [[{"role": "user", "content": "No evidence here."}]],
        "haystack_session_ids": ["s1"],
    }

    query = LongMemEvalLoader.query_from_record(record)

    assert query.is_abstention is True
    assert query.has_retrieval_fixture is False
    assert query.source_turn_ids == ()


def test_target_sets_separate_raw_source_and_canonical_credit():
    query = BenchmarkQuery(
        query_id="q1",
        question="Where does Alex live now?",
        answer="Berlin",
        question_type="knowledge-update",
        source_session_ids=("s_new",),
        source_turn_ids=("q1:s_new:0",),
    )
    raw = MemoryItem(
        memory_id="raw-1",
        kind="raw_turn",
        content="Alex moved from Paris to Berlin.",
        source_turn_ids=("q1:s_new:0",),
        source_session_ids=("s_new",),
        raw=True,
        canonical=False,
    )
    belief = MemoryItem(
        memory_id="belief-1",
        kind="belief",
        content="Alex lives in Berlin.",
        source_turn_ids=("q1:s_new:0",),
        source_session_ids=("s_new",),
        derived_from_memory_ids=("raw-1",),
        raw=False,
        canonical=True,
        currentness="current",
    )
    stale = MemoryItem(
        memory_id="belief-old",
        kind="belief",
        content="Alex lives in Paris.",
        source_turn_ids=("q1:s_new:0",),
        source_session_ids=("s_new",),
        derived_from_memory_ids=("raw-1",),
        raw=False,
        canonical=True,
        currentness="historical",
    )

    target_sets = build_target_sets(query, [raw, belief, stale])

    assert target_sets[Target.RAW] == {"raw-1"}
    assert target_sets[Target.SOURCE] == {"raw-1", "belief-1", "belief-old"}
    assert target_sets[Target.CANONICAL] == {"belief-1"}


def test_rescore_trace_reports_hit_recall_and_ndcg_for_each_target():
    query = BenchmarkQuery(
        query_id="q1",
        question="Where does Alex live now?",
        answer="Berlin",
        question_type="knowledge-update",
        source_session_ids=("s_new",),
        source_turn_ids=("q1:s_new:0",),
    )
    items = [
        MemoryItem("raw-1", "raw_turn", "evidence", ("q1:s_new:0",), ("s_new",), raw=True),
        MemoryItem(
            "canonical-1",
            "canonical_fact",
            "Alex lives in Berlin",
            ("q1:s_new:0",),
            ("s_new",),
            raw=False,
            canonical=True,
        ),
    ]
    trace = RetrievalTrace(
        query_id="q1",
        benchmark="longmemeval_s",
        retriever="test",
        top_k=(
            RetrievedItem(rank=1, memory_id="canonical-1", score=0.9),
            RetrievedItem(rank=2, memory_id="wrong", score=0.8),
            RetrievedItem(rank=3, memory_id="raw-1", score=0.7),
        ),
    )

    scores = rescore_trace(query, items, trace, ks=(1, 3))

    assert scores[Target.CANONICAL].hit_at[1] == 1
    assert scores[Target.CANONICAL].recall_at[1] == 1.0
    assert scores[Target.RAW].hit_at[1] == 0
    assert scores[Target.RAW].recall_at[3] == 1.0
    assert scores[Target.RAW].ndcg_at[3] == ndcg_at_k(["canonical-1", "wrong", "raw-1"], {"raw-1"}, 3)


def test_shared_subset_comparison_drops_queries_missing_a_target():
    q1_raw = {Target.RAW: {"raw-1"}, Target.CANONICAL: {"can-1"}}
    q2_raw = {Target.RAW: {"raw-2"}, Target.CANONICAL: set()}
    traces = {
        "q1": RetrievalTrace("q1", "longmemeval_s", "test", (RetrievedItem(1, "can-1", 1.0),)),
        "q2": RetrievalTrace("q2", "longmemeval_s", "test", (RetrievedItem(1, "raw-2", 1.0),)),
    }

    comparison = compare_targets_on_shared_subset(
        {"q1": q1_raw, "q2": q2_raw},
        traces,
        left=Target.RAW,
        right=Target.CANONICAL,
        k=1,
    )

    assert comparison["query_count"] == 1
    assert comparison["left_recall_at_k"] == 0.0
    assert comparison["right_recall_at_k"] == 1.0
    assert comparison["changed_queries"] == 1


def test_builtin_memory_baseline_ingests_raw_turns_and_emits_retrieval_trace():
    query = LongMemEvalLoader.query_from_record(_sample_record())
    baseline = BuiltInMemoryBaseline()

    memory_items = baseline.ingest(query)
    trace = baseline.retrieve(query, top_k=3)

    assert {item.kind for item in memory_items} == {"raw_turn"}
    assert all(item.raw for item in memory_items)
    assert trace.retriever == "hermes_builtin_lexical"
    assert trace.benchmark == "longmemeval_s"
    assert len(trace.top_k) == 3
    assert trace.top_k[0].memory_id in {item.memory_id for item in memory_items}


def test_metric_helpers_handle_multiple_relevant_items():
    ranked_ids = ["a", "b", "c", "d"]
    relevant_ids = {"b", "d"}

    assert recall_at_k(ranked_ids, relevant_ids, 1) == 0.0
    assert recall_at_k(ranked_ids, relevant_ids, 2) == 0.5
    assert recall_at_k(ranked_ids, relevant_ids, 4) == 1.0
    assert ndcg_at_k(ranked_ids, relevant_ids, 4) > ndcg_at_k(["a", "c", "b", "d"], relevant_ids, 4)


def test_run_builtin_baseline_writes_traces_scores_and_summary(tmp_path):
    dataset_path = tmp_path / "longmemeval_s.json"
    output_path = tmp_path / "baseline.jsonl"
    dataset_path.write_text(__import__("json").dumps([_sample_record()]))

    summary = run_builtin_baseline(dataset_path, output_path=output_path, top_k=5)

    assert summary["benchmark"] == "longmemeval_s"
    assert summary["retriever"] == "hermes_builtin_lexical"
    assert summary["query_count"] == 1
    assert summary["retrieval_fixture_count"] == 1
    assert "raw" in summary["metrics"]
    assert "raw_vs_canonical" in summary["shared_subset"]

    lines = output_path.read_text().splitlines()
    assert len(lines) == 1
    row = __import__("json").loads(lines[0])
    assert row["query_id"] == "q1"
    assert row["trace"]["retriever"] == "hermes_builtin_lexical"
    assert set(row["scores"]) == {"raw", "source", "canonical"}


def test_cli_benchmark_longmemeval_runs_builtin_baseline(tmp_path):
    dataset_path = tmp_path / "longmemeval_s.json"
    output_path = tmp_path / "baseline.jsonl"
    dataset_path.write_text(__import__("json").dumps([_sample_record()]))

    result = runner.invoke(
        app,
        [
            "benchmark-longmemeval",
            str(dataset_path),
            "--output",
            str(output_path),
            "--top-k",
            "5",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "hermes_builtin_lexical" in result.output
    assert "recall@5" in result.output
    assert output_path.exists()
