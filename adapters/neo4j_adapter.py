"""
Neo4j AuraDB adapter.

Shares all Cypher / Bolt logic with :class:`_Neo4jStyleAdapter`;
CognoDB and Neo4j are kept as separate classes so version strings,
platform names, and (in future) Aura-specific workarounds can live in
their own files.
"""

from __future__ import annotations

from ._neo4j_style_base import _Neo4jStyleAdapter


class Neo4jAdapter(_Neo4jStyleAdapter):
    """Adapter targeting Neo4j AuraDB (neo4j+s://…)."""

    @property
    def platform_name(self) -> str:
        return "Neo4j AuraDB"
