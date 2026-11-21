"""Linux AF_UNIX peer identity, fail-closed on unsupported platforms."""

from __future__ import annotations

import socket
import struct
import sys


class PeerCredentialError(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def peer_cred(sock: socket.socket) -> tuple[int, int, int]:
    if sys.platform != "linux" or not hasattr(socket, "SO_PEERCRED"):
        raise PeerCredentialError("E_PEERCRED_UNAVAILABLE")
    try:
        data = sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i"))
        return struct.unpack("3i", data)
    except (OSError, struct.error) as exc:
        raise PeerCredentialError("E_PEERCRED_UNAVAILABLE") from exc


def require_peer_uid(sock: socket.socket, expected_uid: int) -> None:
    if peer_cred(sock)[1] != expected_uid:
        raise PeerCredentialError("E_PEER_UID")


def require_peer_uid_or_none(sock: socket.socket, expected_uid: int | None) -> None:
    if expected_uid is not None:
        require_peer_uid(sock, expected_uid)
