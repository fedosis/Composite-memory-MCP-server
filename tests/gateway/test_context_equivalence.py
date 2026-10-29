"""S08a context mask and typed-key invariants."""

import pytest

from memory_server.application.gateway_retrieval import mask_context_package


def _package(version="1.2.3"):
    return {
        "facts": [
            {
                "id": "f",
                "subject": "secret",
                "predicate": "p",
                "object": "o",
                "dedup_key": "real",
                "source": "s",
                "creator": "c",
                "created_at": "2026-01-01T00:00:00+00:00",
                "updated_at": "2026-01-01T00:00:00+00:00",
                "verification_status": "candidate",
                "lifecycle_state": "active",
                "confidence": 0.8,
                "version": version,
            }
        ],
        "decisions": [
            {
                "id": "d",
                "context": "secret",
                "choice": "choice",
                "reason": "reason",
                "source": "s",
                "creator": "c",
                "rejected_alternatives": ["alt", ""],
                "created_at": "2026-01-01T00:00:00+00:00",
                "updated_at": "2026-01-01T00:00:00+00:00",
                "verification_status": "candidate",
                "lifecycle_state": "active",
                "confidence": 0.8,
                "version": version,
            }
        ],
        "total": 2,
    }


def test_context_mask_is_fresh_and_nested_and_preserves_safe_versions():
    raw = _package()
    masked = mask_context_package(raw)
    assert raw["facts"][0]["dedup_key"] == "real"
    assert masked["facts"][0]["dedup_key"] == "[MASKED]"
    assert masked["decisions"][0]["rejected_alternatives"] == ["[MASKED]", "[MASKED]"]
    assert masked["facts"][0]["version"] == "1.2.3"
    assert list(masked) == list(raw)


@pytest.mark.parametrize(
    "version,expected",
    [
        ("0.1.0", "0.1.0"),
        ("1", "1"),
        ("007", "007"),
        ("1.2.3", "1.2.3"),
        ("v1", "[MASKED]"),
        ("1.2.3-rc1", "[MASKED]"),
        ("", "[MASKED]"),
    ],
)
def test_context_version_rule(version, expected):
    assert mask_context_package(_package(version))["facts"][0]["version"] == expected


def test_context_mask_fails_closed_on_unexpected_shape():
    with pytest.raises(ValueError):
        mask_context_package({"facts": [], "decisions": [], "total": 0, "extra": True})
