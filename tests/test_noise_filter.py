"""Tests for the facts-side noise filter (Card A1)."""

from memory_server.extractors.noise_filter import (
    _clean_str,
    filter_facts,
)


def _fact(subject, predicate, obj):
    return {"subject": subject, "predicate": predicate, "object": obj}


class TestNoiseFilter:
    """Exact ACCEPT/REJECT sets for the facts-side noise filter."""

    # --- REJECT: legacy/newly-rejected noise, fragments, Unicode empties ---

    def test_reject_this_folder_home(self):
        """Subject first token 'This' (demonstrative prefix) -> REJECT."""
        assert filter_facts([_fact("This folder", "is", "home")]) == []

    def test_reject_but_subject(self):
        """Whole-string subject stopword 'but' -> REJECT."""
        assert filter_facts([_fact("but", "is", "never injected or written")]) == []

    def test_reject_that_subject_object_for(self):
        """Subject 'That'; object ends in 'for' -> REJECT."""
        assert filter_facts([_fact("That", "is", "useful evidence for")]) == []

    def test_reject_it_subject(self):
        """Whole-string subject stopword 'It' -> REJECT."""
        assert filter_facts([_fact("It", "is", "done")]) == []

    def test_reject_object_printed_but(self):
        """Object ends in fragment ender 'but' -> REJECT."""
        assert filter_facts([_fact("JSON count", "is", "printed but")]) == []

    def test_reject_single_char_subject_and_object(self):
        """Single-char subject 'X' and stopword object 'a' -> REJECT."""
        assert filter_facts([_fact("X", "is", "a")]) == []

    def test_reject_which_subject(self):
        """Whole-string subject stopword 'which' -> REJECT."""
        assert filter_facts([_fact("which", "is", "repo used")]) == []

    def test_reject_this_repository_prefix(self):
        """Demonstrative-prefix false positive, intentional -> REJECT."""
        assert filter_facts([_fact("this repository", "is", "maintained")]) == []

    def test_reject_object_evidence_for(self):
        """Object ends in fragment ender 'for' -> REJECT."""
        assert filter_facts([_fact("Caddy", "is", "evidence for")]) == []

    def test_reject_object_server_with(self):
        """Object ends in fragment ender 'with' -> REJECT."""
        assert filter_facts([_fact("Caddy", "is", "server with")]) == []

    def test_reject_em_dash_predicate(self):
        """Em dash alone as predicate -> strip -> '' -> REJECT."""
        assert filter_facts([_fact("Caddy", "—", "это сервер")]) == []

    def test_reject_newline_object(self):
        """Raw newline anywhere in object -> REJECT."""
        assert filter_facts([_fact("Python", "is", "fast\nreally")]) == []

    def test_reject_cr_object(self):
        """Raw carriage return anywhere in object -> REJECT."""
        assert filter_facts([_fact("Caddy", "is", "fast\rreally")]) == []

    def test_reject_unicode_punct_alone(self):
        """Edge-junk alone strips to empty -> REJECT (U+061F, quotes, U+060C)."""
        assert filter_facts([_fact("Caddy", "is", "؟")]) == []
        assert filter_facts([_fact("Caddy", "is", "«»")]) == []
        assert filter_facts([_fact("Caddy", "is", "،")]) == []

    def test_reject_predicate_internal_sentence_punct(self):
        """Internal sentence punctuation in predicate -> REJECT (U+FF01, U+061F, U+3002)."""
        assert filter_facts([_fact("Caddy", "сказал！да", "сервер")]) == []
        assert filter_facts([_fact("Caddy", "хорошо؟да", "сервер")]) == []
        assert filter_facts([_fact("Caddy", "работает。быстро", "сервер")]) == []

    def test_reject_non_string_before_stringification(self):
        """Non-string components rejected BEFORE stringification (review #4)."""
        assert filter_facts([_fact(5, "is", "сервер")]) == []
        assert filter_facts([_fact("Caddy", "is", 5)]) == []
        assert filter_facts([_fact("Caddy", 5, "сервер")]) == []
        assert filter_facts([_fact(None, "is", "сервер")]) == []
        assert filter_facts([_fact("Caddy", "is", True)]) == []
        assert filter_facts([_fact("Caddy", "is", [])]) == []

    def test_reject_missing_keys(self):
        """Missing predicate/object keys -> None -> REJECT."""
        assert filter_facts([{"subject": "Docker"}]) == []

    def test_reject_non_dict_rows(self):
        """Non-dict rows rejected deterministically (isinstance guard)."""
        assert filter_facts([["Caddy", "is", "сервер"]]) == []
        assert filter_facts([None]) == []
        assert filter_facts([42]) == []

    def test_reject_single_char_object(self):
        """Object length 1 -> REJECT."""
        assert filter_facts([_fact("Caddy", "is", "X")]) == []

    # --- ACCEPT: legacy fixtures, boundaries, Unicode edge-strip ---

    def test_accept_legacy_docker_container(self):
        """Legacy fixture stays ACCEPT; identity preserved."""
        f = _fact("Docker", "is", "container")
        out = filter_facts([f])
        assert out == [f]
        assert out[0] is f

    def test_accept_legacy_set(self):
        """Every legacy regex-valid fixture stays ACCEPT (D4 boundary).

        filter_facts returns the ORIGINAL dicts (identity, no rebuild), so
        the exact one-item equality target is the input row itself.
        """
        assert filter_facts([_fact("Python", "is", "great")]) == [_fact("Python", "is", "great")]
        assert filter_facts([_fact("Linux", "is", "kernel")]) == [_fact("Linux", "is", "kernel")]
        assert filter_facts([_fact("Caddy", "is", "a web server")]) == [_fact("Caddy", "is", "a web server")]
        assert filter_facts([_fact("MySQL", "is", "fast!")]) == [_fact("MySQL", "is", "fast!")]
        assert filter_facts([_fact("Docker Compose", "is", "installed")]) == [
            _fact("Docker Compose", "is", "installed")
        ]
        assert filter_facts([_fact("Caddy", "is", "«сервер»")]) == [_fact("Caddy", "is", "«сервер»")]
        assert filter_facts([_fact("Python", "is", "быстрый")]) == [_fact("Python", "is", "быстрый")]
        assert filter_facts([_fact("Caddy", "is_a", "web server with automatic HTTPS")]) == [
            _fact("Caddy", "is_a", "web server with automatic HTTPS")
        ]
        assert filter_facts([_fact("the repository", "is", "maintained")]) == [
            _fact("the repository", "is", "maintained")
        ]
        assert filter_facts([_fact("Caddy", "runs on", "контейнерах")]) == [
            _fact("Caddy", "runs on", "контейнерах")
        ]

    def test_accept_mixed_rows_keep_valid_dict(self):
        """Non-dict rows skipped; valid dict kept with identity (review #4)."""
        docker = _fact("Docker", "is", "container")
        out = filter_facts([docker, ["junk"], None, 42])
        assert out == [docker]
        assert out[0] is docker

    def test_accept_unicode_edge_strip(self):
        """Trailing Unicode/ASCII punctuation edge-stripped in the check -> ACCEPT.

        Output rows keep their ORIGINAL values (identity, no rebuild); the
        strip is proven at the _clean_str boundary and by acceptance here.
        """
        assert filter_facts([_fact("Caddy", "is", "сервер؟")]) == [_fact("Caddy", "is", "сервер؟")]
        assert filter_facts([_fact("Caddy", "is", "сервер.")]) == [_fact("Caddy", "is", "сервер.")]
        assert filter_facts([_fact("Caddy", "is", "abc،")]) == [_fact("Caddy", "is", "abc،")]
        assert filter_facts([_fact("«сервер»", "is", "container")]) == [
            _fact("«сервер»", "is", "container")
        ]

    def test_clean_str_boundary(self):
        """Module-level _clean_str proves the strip mechanism directly."""
        assert _clean_str("abc،") == "abc"
        assert _clean_str("сервер؟") == "сервер"
        assert _clean_str("،") == ""

    def test_accept_predicate_boundaries(self):
        """'is' with valid object, 'is_a', multiword, trailing punct, string '5'."""
        assert filter_facts([_fact("Caddy", "is", "container")]) == [_fact("Caddy", "is", "container")]
        assert filter_facts([_fact("Caddy", "is_a", "container")]) == [_fact("Caddy", "is_a", "container")]
        assert filter_facts([_fact("Caddy", "runs on", "container")]) == [_fact("Caddy", "runs on", "container")]
        assert filter_facts([_fact("Caddy", "is.", "container")]) == [_fact("Caddy", "is.", "container")]
        assert filter_facts([_fact("Caddy", "5", "container")]) == [_fact("Caddy", "5", "container")]
