"""Routing rules for pre-embedding exact-match routing (per ADR-005 routing order).

Rules are evaluated before semantic embedding search. If a rule matches,
the query is routed to the specified backend (e.g., SQL, not vector).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class RuleResult:
    """Result of a matching routing rule."""

    route: str
    rule_name: str
    matched_keyword: str
    query: str


@dataclass
class RoutingRule:
    """A single routing rule with keyword matching.

    Args:
        name: Human-readable rule name.
        keywords: List of keywords to match (case-insensitive).
        route: Target route when matched (e.g., "sql", "vector", "graph").
        priority: Higher value = higher priority (evaluated first).
        match_all: If True, ALL keywords must be present (default: any).
    """

    name: str
    keywords: list[str]
    route: str = "sql"
    priority: int = 10
    match_all: bool = False

    def match(self, query: str) -> Optional[RuleResult]:
        """Check if the query matches this rule.

        Args:
            query: User query string.

        Returns:
            RuleResult if matched, None otherwise.
        """
        if not query or not query.strip():
            return None

        query_lower = query.lower().strip()
        matches = []

        for keyword in self.keywords:
            if keyword.lower() in query_lower:
                matches.append(keyword)

        if self.match_all:
            if len(matches) == len(self.keywords):
                return RuleResult(
                    route=self.route,
                    rule_name=self.name,
                    matched_keyword=matches[0],
                    query=query,
                )
        else:
            if matches:
                return RuleResult(
                    route=self.route,
                    rule_name=self.name,
                    matched_keyword=matches[0],
                    query=query,
                )

        return None


class RoutingRuleSet:
    """A collection of routing rules evaluated in priority order."""

    def __init__(self) -> None:
        self.rules: list[RoutingRule] = []

    def add(self, rule: RoutingRule) -> None:
        """Add a routing rule."""
        self.rules.append(rule)

    def clear(self) -> None:
        """Clear all rules."""
        self.rules.clear()

    def evaluate(self, query: str) -> Optional[RuleResult]:
        """Evaluate all rules in priority order (highest first).

        Args:
            query: User query string.

        Returns:
            First matching RuleResult, or None.
        """
        sorted_rules = sorted(self.rules, key=lambda r: r.priority, reverse=True)
        for rule in sorted_rules:
            result = rule.match(query)
            if result is not None:
                return result
        return None

    @staticmethod
    def default() -> RoutingRuleSet:
        """Create a default set of routing rules.

        These rules catch queries that are better answered by exact SQL
        or structured queries rather than semantic vector search.
        """
        rules = RoutingRuleSet()
        rules.add(RoutingRule(
            name="ip_address_query",
            keywords=["ip of", "ip address", "what is the ip", "find ip"],
            route="sql",
            priority=100,
        ))
        rules.add(RoutingRule(
            name="port_query",
            keywords=["port", "which port", "port number"],
            route="sql",
            priority=90,
        ))
        rules.add(RoutingRule(
            name="config_query",
            keywords=["config", "configuration", "show config", "get config"],
            route="sql",
            priority=80,
        ))
        rules.add(RoutingRule(
            name="credential_query",
            keywords=["password", "credential", "api key", "secret", "token", "auth"],
            route="sql",
            priority=70,
        ))
        return rules
