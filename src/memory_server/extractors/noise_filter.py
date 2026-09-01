"""Facts-side noise filter for extraction output (SPEC item 2)."""

# Single source of truth for edge-junk: ASCII + Unicode quotes + brackets +
# hyphen/en/em dash + ASCII punctuation + ellipsis + SPEC-named Unicode
# punctuation (U+061F ?, U+060C ,, U+FF01 !, U+3002 .). Shared verbatim by
# the validator (llm_response imports this constant - one definition, no drift).
EDGE_JUNK = "\"'«»„“”‘’()[]{}-–—.,;:!?…\u061f\u060c\uff01\u3002"

# Predicate INTERNAL sentence punctuation (reject if present after clean):
# ASCII sentence punctuation (parent D3) + SPEC-named Unicode sentence-final
# chars 。！？. Commas (`,` and `،`) and ellipsis (`…`) are edge-strippable
# ONLY, not internal predicate rejectors (word separators / continuation) —
# documented, pinned by fixtures.
PREDICATE_PUNCT = ".!?;:\u3002\uff01\u061f"

PRONOUN_STOPWORDS = frozenset({
    "i", "me", "my", "you", "we", "us", "our", "he", "she", "it", "they",
    "them", "this", "that", "these", "those", "there", "here", "who", "what",
    "when", "where", "why", "which", "and", "but", "or", "so", "the", "a",
    "an",
})
DEMONSTRATIVE_PREFIXES = frozenset({
    "this", "that", "these", "those", "there", "here", "it", "but", "which",
    "what", "who", "when", "where", "and", "so",
})
FRAGMENT_ENDERS = frozenset({
    "and", "or", "but", "because", "that", "to", "for", "of", "the", "a",
    "an", "with", "by", "from", "is", "are", "was", "were", "if", "then",
    "when", "while", "so",
})


def _clean_str(value: str) -> str:
    """Edge-strip EDGE_JUNK + collapse ANY Unicode whitespace. str-only:
    validators gate isinstance BEFORE calling (review #4)."""
    return " ".join(value.strip(EDGE_JUNK).split())


def subject_ok(s: object) -> bool:
    if not isinstance(s, str):            # non-string rejected BEFORE stringification
        return False
    if "\n" in s or "\r" in s:            # newline rule FIRST, on the raw form
        return False
    c = _clean_str(s)
    if len(c) < 2:
        return False
    if c.lower() in PRONOUN_STOPWORDS:    # whole-string pronoun/stopword
        return False
    if c.split()[0].lower() in DEMONSTRATIVE_PREFIXES:
        return False                      # documented intentional rule (parent D3)
    return True


def object_ok(o: object) -> bool:
    if not isinstance(o, str):
        return False
    if "\n" in o or "\r" in o:
        return False
    c = _clean_str(o)
    if len(c) < 2:
        return False
    if c.lower() in PRONOUN_STOPWORDS:
        return False
    if c.split()[-1].lower() in FRAGMENT_ENDERS:  # truncated-fragment guard
        return False
    return True


def predicate_ok(p: object) -> bool:
    if not isinstance(p, str):
        return False
    if "\n" in p or "\r" in p:
        return False
    c = _clean_str(p)
    if not (1 <= len(c) <= 64):
        return False                       # em dash alone -> "" -> REJECT
    if any(ch in c for ch in PREDICATE_PUNCT):
        return False                       # internal sentence punctuation
    return True


def filter_facts(facts: list[dict]) -> list[dict]:
    """All three fields must pass; missing keys -> None -> reject. Non-dict
    rows are REJECTED deterministically: isinstance guard BEFORE `.get` —
    never AttributeError (round-1 finding #4). The `list[dict]` hint is the
    normal contract; the guard keeps the function total over malformed rows.
    Returned dicts are the SAME objects (identity, no rebuild)."""
    return [f for f in facts
            if isinstance(f, dict)
            and subject_ok(f.get("subject"))
            and predicate_ok(f.get("predicate"))
            and object_ok(f.get("object"))]
