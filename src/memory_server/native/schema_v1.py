"""Canonical CMMS schema v1 — the single source for the native tool surface.

The constants below are a mechanical, order-preserving transcription of
``HermesProvider.get_tool_schemas()`` and ``HermesProvider.system_prompt_block()``
as of this revision. They are embedded so that the native local-service
transport (AF_UNIX, sessionless) and the generated plugin artifacts can be
served without importing the provider runtime.

Nothing here may be reworded, reordered, or "improved": the provider is the
source of truth and :func:`assert_matches_provider` is the guard that keeps
this module honest.
"""

from __future__ import annotations

import copy
import difflib
import hashlib
import json
from typing import Any

PROTO_SCHEMA_VERSION: int = 1

# --- begin mechanical transcription of HermesProvider.get_tool_schemas() ----
_TOOL_SCHEMAS_LITERAL: list[dict[str, Any]] = [
    {
        'name': 'invalidate',
        'description': 'Retire a fact or belief, optionally naming its active successor',
        'parameters': {
            'type': 'object',
            'properties': {
                'memory_id': {
                    'type': 'string',
                },
                'memory_type': {
                    'type': 'string',
                    'enum': [
                        'fact',
                        'belief',
                    ],
                    'default': 'fact',
                },
                'reason': {
                    'type': 'string',
                },
                'to_state': {
                    'type': 'string',
                    'enum': [
                        'discarded',
                        'superseded',
                        'stale',
                    ],
                    'default': 'discarded',
                },
                'superseded_by': {
                    'type': 'string',
                    'default': '',
                },
                'expected_version': {
                    'type': 'string',
                    'default': '',
                },
            },
            'required': [
                'memory_id',
                'reason',
            ],
        },
    },
    {
        'name': 'ping',
        'description': 'Connectivity check — returns OK if CMMS is alive',
        'parameters': {
            'type': 'object',
            'properties': {},
            'required': [],
        },
    },
    {
        'name': 'search',
        'description': 'Search stored facts by keyword text with optional subject/predicate filters',
        'parameters': {
            'type': 'object',
            'properties': {
                'include_inactive': {
                    'type': 'boolean',
                    'default': False,
                },
                'query': {
                    'type': 'string',
                    'description': 'Free-text keyword to search across subject/predicate/object',
                },
                'subject': {
                    'type': 'string',
                    'description': 'Optional exact subject filter',
                },
                'predicate': {
                    'type': 'string',
                    'description': 'Optional exact predicate filter',
                },
                'limit': {
                    'type': 'integer',
                    'description': 'Maximum number of results (default 50)',
                    'default': 50,
                },
            },
            'required': [],
        },
    },
    {
        'name': 'remember',
        'description': 'Store a fact with provenance receipt. Writes to SQL and queues async indexing',
        'parameters': {
            'type': 'object',
            'properties': {
                'subject': {
                    'type': 'string',
                    'description': 'The subject of the fact',
                },
                'predicate': {
                    'type': 'string',
                    'description': 'The predicate/relation',
                },
                'object': {
                    'type': 'string',
                    'description': 'The object of the fact',
                },
                'confidence': {
                    'type': 'number',
                    'description': 'Confidence score 0.0-1.0 (default 1.0)',
                    'default': 1.0,
                },
                'source': {
                    'type': 'string',
                    'description': "Source identifier (default 'user')",
                    'default': 'user',
                },
            },
            'required': [
                'subject',
                'predicate',
                'object',
            ],
        },
    },
    {
        'name': 'get_context',
        'description': 'Retrieve structured context about a task — facts and decisions relevant to the turn',
        'parameters': {
            'type': 'object',
            'properties': {
                'task': {
                    'type': 'string',
                    'description': 'The task description or search query',
                },
                'subject': {
                    'type': 'string',
                    'description': 'Optional subject filter',
                },
                'max_results': {
                    'type': 'integer',
                    'description': 'Maximum number of facts to return (default 10)',
                    'default': 10,
                },
            },
            'required': [
                'task',
            ],
        },
    },
    {
        'name': 'learn',
        'description': 'Extract and store facts, decisions, skills, and optionally beliefs from text',
        'parameters': {
            'type': 'object',
            'properties': {
                'text': {
                    'type': 'string',
                    'description': 'Natural language text to extract knowledge from',
                },
                'source': {
                    'type': 'string',
                    'description': "Source identifier (default 'user')",
                    'default': 'user',
                },
                'extract_beliefs': {
                    'type': 'boolean',
                    'description': 'If True, also extract and store beliefs',
                    'default': False,
                },
                'min_belief_confidence': {
                    'type': 'number',
                    'description': 'Minimum confidence to create a belief (default 0.6)',
                    'default': 0.6,
                },
            },
            'required': [
                'text',
            ],
        },
    },
    {
        'name': 'semantic_search',
        'description': 'Semantic search — embed a query and find similar facts by vector similarity',
        'parameters': {
            'type': 'object',
            'properties': {
                'include_inactive': {
                    'type': 'boolean',
                    'default': False,
                },
                'query': {
                    'type': 'string',
                    'description': 'Natural language query text',
                },
                'top_k': {
                    'type': 'integer',
                    'description': 'Maximum number of results (default 10)',
                    'default': 10,
                },
                'score_threshold': {
                    'type': 'number',
                    'description': 'Minimum similarity score 0.0-1.0 (default 0.0)',
                    'default': 0.0,
                },
            },
            'required': [],
        },
    },
    {
        'name': 'graph_search',
        'description': 'Search the knowledge graph for entities/relations by node lookup, pathfinding, or text',
        'parameters': {
            'type': 'object',
            'properties': {
                'query': {
                    'type': 'string',
                    'description': 'Text to extract entity references from',
                },
                'entity_id': {
                    'type': 'string',
                    'description': 'Direct node ID lookup',
                },
                'source_id': {
                    'type': 'string',
                    'description': 'Source entity for pathfinding',
                },
                'target_id': {
                    'type': 'string',
                    'description': 'Target entity for pathfinding',
                },
            },
            'required': [],
        },
    },
    {
        'name': 'route',
        'description': 'Route query through 4-stage router (rules->embeddings->graph->LLM) per ADR-005',
        'parameters': {
            'type': 'object',
            'properties': {
                'include_inactive': {
                    'type': 'boolean',
                    'default': False,
                },
                'query': {
                    'type': 'string',
                    'description': 'Natural language query text',
                },
                'top_k': {
                    'type': 'integer',
                    'description': 'Maximum semantic search results (default 10)',
                    'default': 10,
                },
                'score_threshold': {
                    'type': 'number',
                    'description': 'Minimum similarity score 0.0-1.0 (default 0.0)',
                    'default': 0.0,
                },
            },
            'required': [],
        },
    },
    {
        'name': 'audit',
        'description': 'Run a memory audit for consistency, orphans, confidence distribution, and drift',
        'parameters': {
            'type': 'object',
            'properties': {
                'audit_type': {
                    'type': 'string',
                    'description': "One of 'full', 'consistency', 'orphans', or 'confidence'",
                    'default': 'full',
                    'enum': [
                        'full',
                        'consistency',
                        'orphans',
                        'confidence',
                    ],
                },
            },
            'required': [],
        },
    },
    {
        'name': 'metrics',
        'description': 'Return a Prometheus-formatted snapshot of observability metrics',
        'parameters': {
            'type': 'object',
            'properties': {},
            'required': [],
        },
    },
    {
        'name': 'set_belief',
        'description': 'Create/reinforce/supersede a belief with evidence. Matching beliefs get reinforced',
        'parameters': {
            'type': 'object',
            'properties': {
                'proposition': {
                    'type': 'string',
                    'description': 'The belief proposition text',
                },
                'confidence': {
                    'type': 'number',
                    'description': 'Confidence score 0.0-1.0 (default 0.5)',
                    'default': 0.5,
                },
                'sources': {
                    'type': 'string',
                    'description': 'JSON array of evidence source dicts with source_type/source_id/weight',
                    'default': '[]',
                },
                'tags': {
                    'type': 'string',
                    'description': 'JSON array of tag strings',
                    'default': '[]',
                },
                'source': {
                    'type': 'string',
                    'description': "Source identifier (default 'system')",
                    'default': 'system',
                },
                'replace_belief_id': {
                    'type': 'string',
                    'description': 'If set, supersede the referenced belief and link this new one',
                },
            },
            'required': [
                'proposition',
            ],
        },
    },
    {
        'name': 'get_belief',
        'description': 'Search beliefs — filter by proposition, lifecycle, confidence, tags, source, creator',
        'parameters': {
            'type': 'object',
            'properties': {
                'proposition': {
                    'type': 'string',
                    'description': 'Search proposition text (FTS5 full-text search)',
                },
                'lifecycle_state': {
                    'type': 'string',
                    'description': "Filter by lifecycle state (default 'active')",
                    'default': 'active',
                },
                'min_confidence': {
                    'type': 'number',
                    'description': 'Minimum confidence threshold 0.0-1.0',
                    'default': 0.0,
                },
                'tags': {
                    'type': 'string',
                    'description': 'JSON array of tag strings to filter by',
                },
                'source': {
                    'type': 'string',
                    'description': 'Filter by source identifier',
                },
                'creator': {
                    'type': 'string',
                    'description': 'Filter by creator identifier',
                },
                'source_id': {
                    'type': 'string',
                    'description': "Filter by source_id in the belief's source_ids list",
                },
                'limit': {
                    'type': 'integer',
                    'description': 'Maximum number of results (default 10, max 100)',
                    'default': 10,
                },
            },
            'required': [],
        },
    },
    {
        'name': 'resolve_conflict',
        'description': 'Resolve a conflict between two beliefs (keep_a/b/merge/discard_both, or auto-resolve)',
        'parameters': {
            'type': 'object',
            'properties': {
                'belief_a_id': {
                    'type': 'string',
                    'description': 'UUID of the first belief in the conflict',
                },
                'belief_b_id': {
                    'type': 'string',
                    'description': 'UUID of the second belief in the conflict',
                },
                'resolution': {
                    'type': 'string',
                    'description': 'Strategy: keep_a, keep_b, merge, discard_both',
                    'enum': [
                        'keep_a',
                        'keep_b',
                        'merge',
                        'discard_both',
                    ],
                },
                'new_proposition': {
                    'type': 'string',
                    'description': 'Proposition for a new merged belief (required for merge)',
                },
                'auto_resolve': {
                    'type': 'boolean',
                    'description': 'When True, auto-resolve by confidence threshold',
                    'default': False,
                },
            },
            'required': [
                'belief_a_id',
                'belief_b_id',
                'resolution',
            ],
        },
    },
    {
        'name': 'reflect',
        'description': 'Analyse belief store (overview, contradictions, decay, topics, audit, confidence)',
        'parameters': {
            'type': 'object',
            'properties': {
                'mode': {
                    'type': 'string',
                    'description': 'Mode: overview, contradictions, decay, topics, audit, or confidence',
                    'default': 'overview',
                    'enum': [
                        'overview',
                        'contradictions',
                        'decay',
                        'topics',
                        'evidence_audit',
                        'confidence',
                    ],
                },
                'topic': {
                    'type': 'string',
                    'description': 'Optional topic/tag filter',
                },
                'min_confidence': {
                    'type': 'number',
                    'description': 'Minimum confidence threshold 0.0-1.0',
                    'default': 0.0,
                },
                'limit': {
                    'type': 'integer',
                    'description': 'Max beliefs to analyse (0 = all, default 50)',
                    'default': 50,
                },
            },
            'required': [],
        },
    },
]
# --- end mechanical transcription ------------------------------------------

TOOL_NAMES: tuple[str, ...] = (
    "invalidate",
    "ping",
    "search",
    "remember",
    "get_context",
    "learn",
    "semantic_search",
    "graph_search",
    "route",
    "audit",
    "metrics",
    "set_belief",
    "get_belief",
    "resolve_conflict",
    "reflect",
)

# Deep copies of the literals above, exposed as a tuple. Callers never get a
# reference to the module-private list; :func:`tool_schemas` hands out deep
# copies so a host can mutate its own view freely.
TOOL_SCHEMAS: tuple[dict[str, Any], ...] = tuple(
    copy.deepcopy(schema) for schema in _TOOL_SCHEMAS_LITERAL
)

# --- begin mechanical transcription of HermesProvider.system_prompt_block() --
# Byte-for-byte the provider string, including the trailing newline. The
# source text names 13 of the 15 tools; that is the existing wording and is
# deliberately left untouched.
SYSTEM_PROMPT: str = 'You have access to the Composite Memory MCP Server (CMMS) as a native memory provider. Use these tools to store and retrieve information across sessions: search, remember, learn, get_context, semantic_search, graph_search, route, audit, metrics, set_belief, get_belief, resolve_conflict, reflect.\n'
# --- end mechanical transcription ------------------------------------------


def canonical_payload() -> dict[str, Any]:
    """Return the canonical, transport-neutral schema document."""
    return {
        "v": PROTO_SCHEMA_VERSION,
        "tools": tool_schemas(),
        "prompt": SYSTEM_PROMPT,
    }


def schema_hash() -> str:
    """Return the lowercase hex sha256 of the canonical payload."""
    blob = json.dumps(
        canonical_payload(),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def tool_schemas() -> list[dict[str, Any]]:
    """Return a deep copy of the canonical tool schemas for a host."""
    return copy.deepcopy(list(TOOL_SCHEMAS))


def system_prompt() -> str:
    """Return the canonical static system-prompt block."""
    return SYSTEM_PROMPT


def _unified_diff(expected: Any, actual: Any, label: str) -> str:
    if isinstance(expected, str) and isinstance(actual, str):
        expected_lines = expected.splitlines()
        actual_lines = actual.splitlines()
    else:
        expected_lines = json.dumps(expected, ensure_ascii=False, indent=2, sort_keys=True).splitlines()
        actual_lines = json.dumps(actual, ensure_ascii=False, indent=2, sort_keys=True).splitlines()
    return "\n".join(
        difflib.unified_diff(
            expected_lines,
            actual_lines,
            fromfile=f"canonical:{label}",
            tofile=f"provider:{label}",
            lineterm="",
            n=1,
        )
    )


def assert_matches_provider(provider: Any) -> None:
    """Assert that ``provider`` still matches the embedded schema and prompt.

    Compares the tool names in order, every tool schema, and the system prompt.
    Raises :class:`AssertionError` describing the first mismatch.
    """
    live_prompt = provider.system_prompt_block()
    if live_prompt != SYSTEM_PROMPT:
        raise AssertionError(
            "system prompt drifted from the canonical v1 text\n"
            + _unified_diff(SYSTEM_PROMPT, live_prompt, "SYSTEM_PROMPT")
        )

    live_schemas = provider.get_tool_schemas()
    live_names = [schema.get("name") for schema in live_schemas]
    if live_names != list(TOOL_NAMES):
        raise AssertionError(
            "tool names drifted from the canonical v1 order: "
            f"expected {list(TOOL_NAMES)}, got {live_names}"
        )
    if len(live_schemas) != len(TOOL_SCHEMAS):
        raise AssertionError(
            f"tool schema count drifted: expected {len(TOOL_SCHEMAS)}, got {len(live_schemas)}"
        )

    diffs: list[str] = []
    for expected, actual in zip(TOOL_SCHEMAS, live_schemas):
        if expected != actual:
            name = expected.get("name")
            changed = sorted(
                key
                for key in set(expected) | set(actual)
                if expected.get(key) != actual.get(key)
            )
            diffs.append(
                f"tool '{name}' drifted (fields: {', '.join(changed)})\n"
                + _unified_diff(expected, actual, str(name))
            )
    if diffs:
        raise AssertionError("tool schemas drifted from the canonical v1 literals\n" + "\n".join(diffs))
