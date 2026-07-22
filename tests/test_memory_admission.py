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
