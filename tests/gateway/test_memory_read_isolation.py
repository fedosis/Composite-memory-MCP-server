from datetime import UTC, datetime

from memory_server.application.memory_read_service import CursorCodec, ReadKey
from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import Principal, RequestContext


def _ctx() -> RequestContext:
    principal = Principal(
        "tok", "subject", None, frozenset({"profile"}), frozenset({"memory.read"}), 1, datetime.now(UTC)
    )
    return RequestContext("req", principal, "profile", None, 1.0)


def test_cursor_is_bound_to_profile_and_operation():
    ctx = _ctx()
    codec = CursorCodec(b"x" * 32)
    payload = d.MemoryListQuery(
        cursor=None,
        min_confidence=None,
        max_confidence=None,
        source=None,
        authority_level=None,
        epistemic_status=None,
        created_from=None,
        created_to=None,
        updated_from=None,
        updated_to=None,
        valid_at=None,
    )
    command = d.COMMAND_TYPES["memories.list"](
        operation="memories.list", profile_id="profile", path_ids={}, payload=payload
    )
    now = datetime.now(UTC)
    token = codec.issue(
        ctx,
        command,
        last=ReadKey("2026-01-01T00:00:00+00:00", "fact", "m"),
        upper=ReadKey("2026-01-02T00:00:00+00:00", "fact", "z"),
        limit=50,
        now=now,
    )
    assert codec.validate(ctx.principal, command, token, now=now)["limit"] == 50
    other = command.model_copy(update={"profile_id": "other"})
    try:
        codec.validate(ctx.principal, other, token, now=now)
    except Exception as exc:
        assert getattr(exc, "code", None) == "E_CURSOR_INVALID"
    else:
        raise AssertionError("foreign profile cursor accepted")
