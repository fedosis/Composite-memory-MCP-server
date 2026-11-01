"""S14 concurrency and cancellation acceptance tests."""
from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_two_clients_barrier_same_observed_version():
    pytest.skip("candidate lifecycle integration is covered after the RED discriminator")


@pytest.mark.asyncio
async def test_cancel_before_commit_rolls_back_all_effects():
    pytest.skip("candidate lifecycle integration is covered after the RED discriminator")
