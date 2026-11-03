"""S16A existing HTTP read compatibility; no relation write routes."""

import pytest

from tests.gateway.test_graph_reads import BASIC, relation_auth, seed
from tests.gateway.test_http_retrieval import http_request, live_server
from tests.gateway.test_slice_runtime import offline_embedder as offline_embedder


@pytest.mark.asyncio
async def test_relation_get_etag_and_canonical_revision_readback(tmp_path):
    async with live_server(tmp_path) as (holder, port, _):
        auth = await relation_auth(holder)
        await seed(holder, auth)
        headers = {}
        status, item = await http_request(
            port,
            "GET",
            "/api/v1/profiles/alpha/relations/" + BASIC,
            headers=auth,
            response_headers=headers,
        )
        assert status == 200, item
        assert item["revision"] == 1, "S16A_VIRTUAL_REVISION_ONE"
        assert headers.get("etag") == f'"r:{BASIC}:1"', "S16A_RELATION_ETAG"
        assert headers["cache-control"] == "no-store"
