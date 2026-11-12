"""Synthetic JSON fixtures only; no runtime/providers or settings changes."""

import pytest


@pytest.fixture
def utc_timestamp():
    return "2026-10-28T00:00:00Z"
