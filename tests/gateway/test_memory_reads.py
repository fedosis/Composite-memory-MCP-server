from datetime import UTC, datetime
from types import SimpleNamespace

from memory_server.application.memory_read_service import ReadRecord, classify_read_content, project_for_caller
from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import Principal, RequestContext


def _ctx(*permissions: str) -> RequestContext:
    principal = Principal("tok", "subject", None, frozenset({"p"}), frozenset(permissions), 1, datetime.now(UTC))
    return RequestContext("req", principal, "p", None, 1.0)


def _memory() -> d.Memory:
    now = datetime.now(UTC)
    return d.Memory(
        id="m",
        memory_type="fact",
        content="canary",
        payload=d.FactPayload(subject="s", predicate="p", object="canary"),
        confidence=0.9,
        lifecycle_state="active",
        source_version="1",
        source="src",
        creator="creator",
        created_at=now,
        updated_at=now,
        valid_from=None,
        valid_to=None,
        epistemic_status=None,
        authority_level=None,
        risk_tags=[],
        receipt_id=None,
        masked=False,
        projection=d.ItemProjection(vector="unknown", graph="unknown", outbox="unknown"),
    )


def test_get_projection_is_always_masked():
    result = project_for_caller(
        _ctx("memory.read", "memory.reveal"), "memories.get", ReadRecord(_memory(), "fact"), "clean"
    )
    assert result.masked is True
    assert result.content is None and result.payload is None
    assert "canary" not in result.model_dump_json()


def test_reveal_clean_projection_returns_content():
    result = project_for_caller(_ctx("memory.reveal"), "memories.reveal", ReadRecord(_memory(), "fact"), "clean")
    assert result.content == "canary"


def test_unknown_classifier_fails_closed():
    record = ReadRecord(SimpleNamespace(bad=object()), "fact")
    assert classify_read_content(record, SimpleNamespace(admit=lambda *a, **k: True)) == "unknown"
