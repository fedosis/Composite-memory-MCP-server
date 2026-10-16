"""Closed error set for the transport-free application layer (C06).

The codes below live deliberately OUTSIDE the closed diagnostics catalogue
(``profile_migration.py:431-448``): none starts with ``E_LOCK_`` or
``E_WRITER_``, none is registered in the catalogue, and none reaches the CLI
table — ``exit_code_for_diagnostic`` (``profile_migration.py:473-486``) is
unaffected, so the 52-entry catalogue and every table derived from it stay
byte-identical.

Secrets policy (``settings.py:13-16``): ``__str__`` renders the message only —
never an environment value, never a credential, never a provider handle or a
connection URL.
"""

from __future__ import annotations


class ApplicationError(Exception):
    """Base class for the application layer's closed error set."""

    code = "E_APPLICATION"

    def __init__(self, message: str = "", *, code: str | None = None) -> None:
        super().__init__(message)
        if code is not None:
            self.code = code

    def __str__(self) -> str:
        if self.args:
            return str(self.args[0])
        return self.code


class InvalidCommandError(ApplicationError, ValueError):
    """A command DTO failed validation.

    It subclasses ``ValueError`` so every existing caller that catches
    ``ValueError`` — including ``pytest.raises(ValueError, match="subject")``
    and ``match="confidence"`` at ``tests/test_remember.py:179``, ``:188`` —
    keeps working unchanged. Messages mirror
    ``services/ingestion_service.py:312-319`` exactly.
    """

    code = "E_APPLICATION_INVALID_COMMAND"


class UnknownDependencyError(ApplicationError):
    """A required explicit dependency was not supplied to the application."""

    code = "E_APPLICATION_UNKNOWN_DEPENDENCY"
