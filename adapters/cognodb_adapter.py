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

    def get_footprint(self) -> str | dict[str, object]:
        """Return a clearly labelled graph-count proxy when bytes are hidden.

        CognoDB does not document a Bolt-exposed storage metric.  Never turn
        graph counts into an invented MB value: they are retained only as an
        explicit proxy until a provider-console reading is recorded manually.
        """
        try:
            with self._session() as session:
                record = session.run(
                    "CALL db.stats.retrieve('GRAPH COUNTS') YIELD data "
                    "RETURN data LIMIT 1"
                ).single()
                data = record.get("data") if record is not None else None
                if isinstance(data, dict):
                    nodes = data.get("nodes") or data.get("nodeCount")
                    relationships = (
                        data.get("relationships")
                        or data.get("relationshipCount")
                    )
                    if nodes is not None or relationships is not None:
                        return {
                            "storage_mb": None,
                            "memory_mb": None,
                            "nodes": int(nodes or 0),
                            "relationships": int(relationships or 0),
                            "notes": (
                                "Count-based proxy, not a direct byte-size "
                                "measurement. Obtained via "
                                "CALL db.stats.retrieve('GRAPH COUNTS')."
                            ),
                        }
        except Exception:
            pass

        # The procedure is edition/provider dependent; a portable Cypher
        # fallback still gives an honest count-based proxy when it is missing
        # or returns an unexpected shape.
        try:
            with self._session() as session:
                record = session.run(
                    "MATCH (n) WITH count(n) AS nodes "
                    "OPTIONAL MATCH ()-[r]->() "
                    "RETURN nodes, count(r) AS relationships"
                ).single()
                if record is not None:
                    return {
                        "storage_mb": None,
                        "memory_mb": None,
                        "nodes": int(record["nodes"]),
                        "relationships": int(record["relationships"]),
                        "notes": (
                            "Count-based proxy, not a direct byte-size "
                            "measurement. Obtained with Cypher counts after "
                            "db.stats.retrieve was unavailable."
                        ),
                    }
        except Exception:
            pass
        return "Not Observable"
