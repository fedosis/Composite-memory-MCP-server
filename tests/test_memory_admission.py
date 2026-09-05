"""Tests for v0.11 memory admission gate, tagging, TTL prune, and MEMORY.md import."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from memory_server.admission import MemoryAdmissionGate, MemoryTag
from memory_server.api.bulk_import import import_memory_md
from memory_server.api.remember import remember
from memory_server.providers.sqlite_provider import SQLiteProvider


@pytest.fixture
async def provider():
    p = SQLiteProvider(url="sqlite+aiosqlite:///:memory:")
    await p.initialize()
    yield p
    await p.close()


def test_admission_gate_rejects_ephemeral_noise():
    gate = MemoryAdmissionGate()

    decision = gate.classify("thanks, ok")

    assert decision.admitted is False
    assert decision.tag == MemoryTag.EPHEMERAL
    assert decision.ttl_days == 1
    assert "low_signal" in decision.reason_codes


def test_admission_gate_tags_durable_preference_with_structured_metadata():
    gate = MemoryAdmissionGate()

    decision = gate.classify("User prefers concise terminal-friendly responses")

    assert decision.admitted is True
    assert decision.tag == MemoryTag.DURABLE
    assert decision.ttl_days == 365
    assert decision.metadata["memory_kind"] == "user_preference_style"
    assert decision.metadata["authority_level"] == "confirmed_user_preference"
    assert "style_only" in decision.metadata["admission_tags"]
    assert "tool_parameter_ok" not in decision.metadata["admission_tags"]


def test_admission_gate_tags_important_policy_without_ttl():
    gate = MemoryAdmissionGate()

    decision = gate.classify("IMPORTANT: Never disable logging or rollback safeguards")

    assert decision.admitted is True
    assert decision.tag == MemoryTag.IMPORTANT
    assert decision.ttl_days is None
    assert decision.metadata["memory_kind"] == "system_policy"
    assert "logging_sensitive" in decision.metadata["risk_tags"]
    assert "security_sensitive" in decision.metadata["risk_tags"]


def test_admission_gate_force_admits_ephemeral():
    """force=True admits low-signal text while keeping EPHEMERAL tag and TTL."""
    gate = MemoryAdmissionGate()

    decision = gate.classify("thanks, ok", force=True)

    assert decision.admitted is True
    assert decision.tag == MemoryTag.EPHEMERAL
    assert decision.ttl_days == 1
    assert "low_signal" in decision.reason_codes


def test_admission_gate_rejects_empty_or_whitespace():
    """Empty and whitespace-only input is rejected as low_signal."""
    gate = MemoryAdmissionGate()

    empty = gate.classify("")
    ws = gate.classify("   ")

    for d in (empty, ws):
        assert d.admitted is False
        assert d.tag == MemoryTag.EPHEMERAL
        assert d.ttl_days == 1
        assert "low_signal" in d.reason_codes


def test_admission_gate_tags_transient_terms():
    """Text with transient/temporary terms gets EPHEMERAL tag regardless of length."""
    gate = MemoryAdmissionGate()

    decision = gate.classify("temporary scratch note for today")

    assert decision.admitted is False
    assert decision.tag == MemoryTag.EPHEMERAL
    assert decision.ttl_days == 1
    assert "low_signal" in decision.reason_codes


@pytest.mark.asyncio
async def test_remember_with_admission_metadata_persists_tag_and_ttl(provider):
    gate = MemoryAdmissionGate()
    decision = gate.classify("User prefers concise terminal-friendly responses")

    result = await remember(
        provider,
        subject="User",
        predicate="prefers",
        object="concise terminal-friendly responses",
        source="test",
        admission=decision,
    )

    receipt = result["receipt"]
    assert receipt.history
    metadata = receipt.history[0]["metadata"]
    assert metadata["admission"]["tag"] == "durable"
    assert metadata["admission"]["ttl_days"] == 365
    assert metadata["admission"]["memory_kind"] == "user_preference_style"
    assert metadata["admission"]["state_status"] == "active"
    assert metadata["admission"]["expires_at"] is not None


@pytest.mark.asyncio
async def test_prune_expired_memories_archives_fact_and_receipt(provider):
    past = datetime.now(timezone.utc) - timedelta(days=2)
    result = await remember(
        provider,
        subject="Temporary note",
        predicate="is",
        object="expired",
        source="test",
        admission=MemoryAdmissionGate().classify("temporary note", now=past, force=True),
    )
    fact_id = result["fact"].id

    summary = await provider.prune_expired_memories(now=datetime.now(timezone.utc))

    assert summary["pruned"] == 1
    fact = await provider.get_fact(fact_id)
    receipt = await provider.get_receipt(fact_id)
    assert fact.lifecycle_state == "archived"
    assert receipt.lifecycle_state == "archived"


@pytest.mark.asyncio
async def test_bulk_import_memory_md_skips_ephemeral_and_imports_durable(tmp_path, provider):
    memory_md = tmp_path / "MEMORY.md"
    memory_md.write_text(
        """
# MEMORY

- thanks, ok
- User prefers concise terminal-friendly responses.
- IMPORTANT: Never disable logging or rollback safeguards.
""".strip(),
        encoding="utf-8",
    )

    result = await import_memory_md(provider, memory_md, source="MEMORY.md")

    assert result["imported"] == 2
    assert result["skipped"] == 1
    facts = await provider.search_facts(source="MEMORY.md")
    objects = {fact.object for fact in facts}
    assert "User prefers concise terminal-friendly responses." in objects
    assert "IMPORTANT: Never disable logging or rollback safeguards." in objects


def test_embedded_words_are_not_false_positives():
    """SVC-4: 'knows'/'nowhere' must not trip the transient 'now' term and
    'mustard' must not trip the important 'must' term."""
    gate = MemoryAdmissionGate()

    knows = gate.classify("User knows the server IP address")
    assert knows.admitted is True
    assert knows.tag is not MemoryTag.EPHEMERAL
    assert "low_signal" not in knows.reason_codes

    nowhere = gate.classify("The fix is nowhere near ready")
    assert nowhere.admitted is True
    assert nowhere.tag is not MemoryTag.EPHEMERAL

    mustard = gate.classify("The mustard jar is on the shelf")
    assert mustard.tag is not MemoryTag.IMPORTANT, (
        "'mustard' embeds 'must' and must not classify as IMPORTANT"
    )
    assert mustard.admitted is True


def test_genuine_now_and_must_still_match():
    """Real 'now' (transient) and 'must' (important) words still classify."""
    gate = MemoryAdmissionGate()

    now_text = gate.classify("Backup the config now")
    assert now_text.tag is MemoryTag.EPHEMERAL
    assert "low_signal" in now_text.reason_codes

    must_text = gate.classify("You must back up before migrating")
    assert must_text.tag is MemoryTag.IMPORTANT
    assert "explicit_importance" in must_text.reason_codes


def test_punctuation_is_covered():
    """Punctuation must not hide noise/importance/transience."""
    gate = MemoryAdmissionGate()

    assert gate.classify("ok.").tag is MemoryTag.EPHEMERAL
    assert gate.classify("thanks!").tag is MemoryTag.EPHEMERAL
    # 'now' before '!' is a genuine transient mention.
    assert gate.classify("Write scratch note now!").tag is MemoryTag.EPHEMERAL
    # Multi-word phrase 'do not' with punctuation.
    decision = gate.classify("Do NOT disable logging.")
    assert decision.tag is MemoryTag.IMPORTANT
    assert decision.metadata["memory_kind"] == "system_policy"
    # Multi-word noise phrase with punctuation.
    assert gate.classify("got it.").tag is MemoryTag.EPHEMERAL
    assert gate.classify("thank you!").tag is MemoryTag.EPHEMERAL


def test_unicode_phrase_matching():
    """Cyrillic transient/important terms match as whole words."""
    gate = MemoryAdmissionGate()

    today = gate.classify("сегодня это черновик заметки")
    assert today.tag is MemoryTag.EPHEMERAL, "today/draft in Russian is transient"

    important = gate.classify("обязательно сделать резервную копию")
    assert important.tag is MemoryTag.IMPORTANT


def test_receipt_updated_at_default_is_utc_aware():
    """SVC-7: MemoryReceipt.updated_at defaults to a UTC-aware datetime."""
    from memory_server.models.receipt import MemoryReceipt

    receipt = MemoryReceipt(
        id="r1",
        memory_type="fact",
        source="src",
        created_by="test",
        timestamp=datetime.now(timezone.utc),
    )
    assert receipt.updated_at.tzinfo is not None
    assert receipt.updated_at.utcoffset() == timedelta(0)
