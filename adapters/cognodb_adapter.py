"""
CognoDB Cloud adapter.

CognoDB Cloud exposes a **Bolt-compatible endpoint** that is compatible
with the official ``neo4j`` Python driver, so the implementation is a
thin wrapper around :class:`_Neo4jStyleAdapter` which holds all shared
Cypher logic.
"""

from __future__ import annotations

from ._neo4j_style_base import _Neo4jStyleAdapter


class CognoDBAdapter(_Neo4jStyleAdapter):
    """Adapter targeting CognoDB Cloud (bolt+s://…)."""

    @property
    def platform_name(self) -> str:
        return "CognoDB Cloud"
