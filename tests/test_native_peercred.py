"""Linux peer-credential admission tests."""

import os
import socket
from contextlib import closing

import pytest

from memory_server.native import peercred


def test_same_uid_accepted():
    left, right = socket.socketpair()
    with closing(left), closing(right):
        peercred.require_peer_uid(left, os.geteuid())
        assert peercred.peer_cred(right)[1] == os.geteuid()


def test_foreign_uid_rejected(monkeypatch):
    monkeypatch.setattr(peercred, "peer_cred", lambda sock: (123, os.geteuid() + 1, 123))
    left, right = socket.socketpair()
    with closing(left), closing(right):
        with pytest.raises(peercred.PeerCredentialError) as exc:
            peercred.require_peer_uid(left, os.geteuid())
    assert exc.value.code == "E_PEER_UID"


def test_unavailable_fails_closed(monkeypatch):
    monkeypatch.setattr(peercred.sys, "platform", "not-linux")
    left, right = socket.socketpair()
    with closing(left), closing(right):
        with pytest.raises(peercred.PeerCredentialError) as exc:
            peercred.peer_cred(left)
    assert exc.value.code == "E_PEERCRED_UNAVAILABLE"
