"""Card 3 remainder: DecayEngine derives defaults from Settings (drift tests)."""
from memory_server.evaluation.decay import (
    ARCHIVE_RATIO,
    DEFAULT_ARCHIVE_THRESHOLD,
    FORGOTTEN_RATIO,
    PER_TYPE_TTL,
    STALE_RATIO,
    DecayEngine,
)
from memory_server.settings import get_settings


def test_module_aliases_match_settings():
    assert PER_TYPE_TTL == get_settings().ttl_days
    assert DEFAULT_ARCHIVE_THRESHOLD == get_settings().decay_archive_threshold
    assert STALE_RATIO == get_settings().decay_stale_ratio
    assert ARCHIVE_RATIO == get_settings().decay_archive_ratio
    assert FORGOTTEN_RATIO == get_settings().decay_forgotten_ratio


def test_defaults_equal_old_constants():
    engine = DecayEngine()
    assert engine.get_ttl("fact") == 90.0 and engine.get_ttl("belief") == 180.0
    assert engine._archive_threshold == 0.2
    assert engine._stale_ratio == 0.7 and engine._archive_ratio == 1.0
    assert engine._forgotten_ratio == 2.0 and engine._default_ttl_days == 90.0
    assert engine._decay_base == 2.0 and engine._confidence_floor == 0.1


def test_env_override_changes_defaults(monkeypatch):
    monkeypatch.setenv("MEMORY_SERVER_TTL_DAYS__BELIEF", "30")
    get_settings.cache_clear()
    engine = DecayEngine()                       # live resolution at __init__
    assert engine.get_ttl("belief") == 30.0
    assert engine._stale_ratio == 0.7            # untouched field keeps default


def test_env_override_ratio(monkeypatch):
    monkeypatch.setenv("MEMORY_SERVER_DECAY_STALE_RATIO", "0.5")
    get_settings.cache_clear()
    assert DecayEngine()._stale_ratio == 0.5


def test_partial_ttl_override_omits_belief_falls_back(monkeypatch):
    """Partial settings dict without 'belief' must not KeyError:
    get_ttl('belief') falls back to decay_default_ttl_days (90.0)."""
    monkeypatch.setenv("MEMORY_SERVER_TTL_DAYS", '{"fact": 30.0}')
    get_settings.cache_clear()
    engine = DecayEngine()
    assert engine.get_ttl("fact") == 30.0
    assert engine.get_ttl("belief") == 90.0   # fallback, not 180.0


def test_constructor_overrides_still_win(monkeypatch):
    monkeypatch.setenv("MEMORY_SERVER_TTL_DAYS__BELIEF", "30")
    monkeypatch.setenv("MEMORY_SERVER_DECAY_STALE_RATIO", "0.5")
    get_settings.cache_clear()
    engine = DecayEngine(per_type_ttl={"fact": 1.0}, stale_ratio=0.9,
                         archive_threshold=0.8, decay_base=3.0)
    assert engine.get_ttl("fact") == 1.0
    assert engine.get_ttl("belief") == 30.0      # un-overridden key from Settings
    assert engine._stale_ratio == 0.9 and engine._archive_threshold == 0.8
    assert engine._decay_base == 3.0
