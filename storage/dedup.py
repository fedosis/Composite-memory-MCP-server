"""Normalized decision dedup keys — shared by the write path, read path, and DB.

Why a normalized key (W1)
-------------------------
The exact ``(context, choice)`` match is too brittle for the observed
production recurrence: the regex extractor captures a *growing parenthetical*
into ``choice`` (e.g. the travel-agent decision re-ingested with choice
lengths 1381/1393/3610/6401 chars), so exact equality never fires and the
context block floods with near-duplicates of the same decision. The dedup key
collapses whitespace and truncates to a stable prefix so variants of the same
decision collapse, while genuinely different decisions (different prefixes)
are never merged.

Why a 200-char prefix
---------------------
The live travel-agent variants share their first 200 collapsed characters and
differ only in the tail (the growing parenthetical list), so a 200-char prefix
absorbs the observed drift. 200 chars is also long enough that two genuinely
distinct decisions essentially never share their first ~200 collapsed
characters, making false merges vanishingly unlikely. This is a conservative
default: truncation only merges rows whose first 200 collapsed chars are
byte-identical. Note the flip side: choices SHORTER than 200 chars keep
exact-match semantics (a 40-char choice and a 300-char choice whose text
starts identically do NOT collapse) — that is intentional, since collapsing
short choices that merely start alike risks merging genuinely different
decisions (e.g. "use Caddy" vs "use Caddy and Nginx").

Consistency contract
--------------------
The DB-level uniqueness guard is a *partial unique index* on
``(context, dedup_key)`` restricted to ACTIVE lifecycle states (see
``storage.models.decision.DecisionORM`` and the
``add_decision_unique_constraint`` migration). ``find_existing()`` and the
read-path dedup in ``get_context.py`` must use the SAME key
(``decision_dedup_key``) so the write-path check, the read-path collapse, and
the DB constraint all agree. The index is partial so a rejected/archived row
does NOT block re-ingestion of the same key (W3).
"""

#: Stable prefix length (in chars) used for the normalized choice key.
DECISION_DEDUP_PREFIX_LEN = 200

#: Lifecycle states that participate in dedup / uniqueness (W3).
ACTIVE_LIFECYCLE_STATES = ("candidate", "validated", "active")


def normalize_choice(choice: object, prefix_len: int = DECISION_DEDUP_PREFIX_LEN) -> str:
    """Collapse whitespace and truncate ``choice`` to a stable prefix.

    ``None`` (emitted by some LLM extractors) is coerced to ``""`` so the
    dedup key construction never crashes on an explicit ``None`` value.
    """
    if choice is None:
        return ""
    collapsed = " ".join(str(choice).split())
    return collapsed[:prefix_len]


def decision_dedup_key(context: object, choice: object) -> tuple[str, str]:
    """Return the normalized dedup key ``(context.strip(), choice_normalized)``.

    This is the single source of truth used by:
    - ``DecisionRepository.find_existing`` (write-path dedup)
    - ``get_context`` read-path dedup
    - the ``dedup_key`` column + partial unique index in ``DecisionORM``
    """
    return (str(context or "").strip(), normalize_choice(choice))
