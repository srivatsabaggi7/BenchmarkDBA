"""
Memgraph adapter (Docker, bolt://localhost:7687).

Memgraph is Bolt/Cypher compatible but its DDL (constraints, indices,
introspection) and some system procedures differ from Neo4j, so we
override only the relevant hooks inherited from
:class:`_Neo4jStyleAdapter` while re-using all DML.
"""

from __future__ import annotations

from neo4j.exceptions import ClientError, Neo4jError

from ._neo4j_style_base import _Neo4jStyleAdapter


class MemgraphAdapter(_Neo4jStyleAdapter):
    """Adapter targeting a locally running Memgraph (Docker) instance."""

    #: Memgraph does not support `FOR (n:Label) ...` style constraints yet.
    #: We use the Memgraph-native CREATE INDEX ... syntax instead.
    _liveness_check_timeout_s: float = 15.0

    @property
    def platform_name(self) -> str:
        return "Memgraph"

    # ------------------------------------------------------------------
    # Memgraph DDL overrides
    # ------------------------------------------------------------------

    def _drop_existing_indices_statements(self) -> list[str]:
        # Memgraph: DROP INDEX ON :Label(attr); no constraint syntax yet
        return [
            "DROP INDEX ON :User(id)",
            "DROP INDEX ON :User(reputation_score)",
        ]

    def _create_vertex_id_unique_constraint(self) -> str:
        # Memgraph does not currently implement explicit UNIQUE constraints
        # via Cypher DDL – uniqueness is enforced by the indexed attribute.
        # Return a no-op that still consumes a result set.
        return "RETURN 'no explicit constraint in memgraph' AS note LIMIT 1"

    def _create_vertex_id_index(self) -> str:
        return "CREATE INDEX ON :User(id)"

    def _create_reputation_index(self) -> str:
        return "CREATE INDEX ON :User(reputation_score)"

    def _clear_cypher_batch(self) -> str:
        # Memgraph supports DETACH DELETE identically
        return super()._clear_cypher_batch()

    # ------------------------------------------------------------------
    # Override connect() to report Memgraph version correctly
    # ------------------------------------------------------------------

    def connect(self) -> None:
        # Run shared connect() first
        super().connect()
        # Try Memgraph-specific procedure
        try:
            with self._session() as s:
                rec = s.run(
                    "SHOW STORAGE INFO RETURN storage_info AS info "
                    "LIMIT 1"
                ).single()
                if rec is not None:
                    info = rec.get("info") or {}
                    if isinstance(info, dict):
                        v = info.get("memgraph_version", "Unknown")
                        self._observed_version = f"Memgraph {v}"
        except (Neo4jError, ClientError, Exception):
            # Fallback: try show database version info if older build
            try:
                with self._session() as s:
                    rec = s.run("SHOW DATABASE VERSION YIELD version RETURN version")
                    if rec is not None:
                        r = rec.single()
                        if r is not None:
                            self._observed_version = f"Memgraph {r['version']}"
            except Exception:
                pass

    # ------------------------------------------------------------------
    # get_footprint – try Memgraph's SHOW STORAGE INFO
    # ------------------------------------------------------------------

    def get_footprint(self) -> str:
        try:
            with self._session() as s:
                rows = list(s.run("SHOW STORAGE INFO"))
                if rows:
                    # Flatten list of storage-info rows into a friendly string
                    summary = ", ".join(
                        f"{r[0]}={r[1]}" if len(r) >= 2 else str(r)
                        for r in rows[:8]
                    )
                    return "Memgraph storage: " + summary
        except (Neo4jError, ClientError, Exception):
            pass
        return super().get_footprint()
