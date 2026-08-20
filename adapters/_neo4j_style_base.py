"""
Shared base for **Bolt / Cypher compatible** graph databases.

Concrete targets that inherit from this base:
  - CognoDB Cloud (fully Neo4j-driver compatible via Bolt)
  - Neo4j AuraDB  (official driver, native)
  - Memgraph      (bolt://localhost:7687, neo4j-driver compatible with
                    small DDL variations – exposed via overridable hooks)

Subclasses override only metadata + DDL hooks when needed;
the entire query surface (lookups / traversals / aggregate /
write_query / ingestion) is implemented once here.
"""

from __future__ import annotations

import gc
import logging
import time
from collections import deque
from itertools import islice
from typing import Any, Iterable, Iterator

from neo4j import (
    Auth,
    GraphDatabase,
    basic_auth,
    ManagedTransaction,
    Record,
    Session,
)
from neo4j.exceptions import (
    AuthError,
    ClientError,
    DatabaseError,
    Neo4jError,
    ServiceUnavailable,
    TransientError,
)

from core.base_adapter import BaseGraphAdapter, IngestResult

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _chunked(iterable: Iterable[Any], size: int) -> Iterator[list[Any]]:
    """Yield successive ``size``-length chunks from ``iterable``."""
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            return
        yield chunk


def _record_to_dict(rec: Record) -> dict[str, Any]:
    """Convert a driver ``Record`` into a JSON-serialisable dict.

    Nodes/Relationships are projected into plain dicts with their
    element_id / type info preserved (neo4j v5 uses ``element_id``).
    """
    out: dict[str, Any] = {}
    for key, value in rec.items():
        out[key] = _to_plain(value)
    return out


def _node_to_plain(node) -> dict[str, Any]:
    props = dict(node)
    return {
        "id": props.get("id") or getattr(node, "element_id", str(node.id)),
        "labels": sorted(list(getattr(node, "labels", []))),
        "properties": props,
    }


def _rel_to_plain(rel) -> dict[str, Any]:
    return {
        "type": str(getattr(rel, "type", type(rel).__name__)),
        "properties": dict(rel),
    }


def _to_plain(value: Any) -> Any:
    if hasattr(value, "labels") and hasattr(value, "_properties"):
        return _node_to_plain(value)
    if hasattr(value, "type") and hasattr(value, "_properties") and not isinstance(value, str):
        return _rel_to_plain(value)
    if isinstance(value, (list, tuple, set)):
        return [_to_plain(v) for v in value]
    if isinstance(value, dict):
        return {k: _to_plain(v) for k, v in value.items()}
    return value


# ---------------------------------------------------------------------------
# Base class
# ---------------------------------------------------------------------------


class _Neo4jStyleAdapter(BaseGraphAdapter):
    """Shared implementation for Bolt+Cypher databases."""

    #: Override per subclass to tune driver behaviour.
    _liveness_check_timeout_s: float = 30.0
    _connection_acquisition_timeout_s: float = 30.0
    _max_connection_pool_size: int = 100

    def __init__(
        self,
        bolt_uri: str,
        username: str,
        password: str,
        database: str | None = None,
    ) -> None:
        self._bolt_uri = bolt_uri
        self._username = username
        self._password = password
        self._database = database
        self._driver = None
        self._observed_version: str = "Unknown"

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    @property
    def platform_version(self) -> str:
        return self._observed_version

    # ------------------------------------------------------------------
    # DDL overridables (Neo4j vs Memgraph differ here)
    # ------------------------------------------------------------------

    def _drop_existing_indices_statements(self) -> list[str]:
        """Cypher statements that drop all indices/constraints.

        Memgraph overrides this with Memgraph-specific introspection.
        """
        return [
            "CALL apoc.schema.assert({}, {}) YIELD label, key RETURN label, key",
        ]

    def _create_vertex_id_unique_constraint(self) -> str:
        return (
            "CREATE CONSTRAINT user_id_unique IF NOT EXISTS "
            "FOR (u:User) REQUIRE u.id IS UNIQUE"
        )

    def _create_vertex_id_index(self) -> str:
        return (
            "CREATE INDEX user_id_idx IF NOT EXISTS "
            "FOR (u:User) ON (u.id)"
        )

    def _create_reputation_index(self) -> str:
        return (
            "CREATE INDEX user_reputation_idx IF NOT EXISTS "
            "FOR (u:User) ON (u.reputation_score)"
        )

    def _traversal_cypher(self, hops: int) -> str:
        """Return a parameterised traversal Cypher statement for ``hops``.

        Parameters: ``$start``.
        Returns:    ``neighbors`` as a list of projected node dicts.
        """
        depth = "*1.." + str(hops)
        return (
            "MATCH path = (start:User {id: $start})-["
            + depth
            + "]-(n:User) "
            "WITH DISTINCT n "
            "RETURN collect(properties(n)) AS neighbors"
        )

    def _clear_cypher_batch(self) -> str:
        return """
        MATCH (n)
        WITH n LIMIT $batch
        DETACH DELETE n
        RETURN count(*) AS deleted
        """

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def _build_auth(self) -> Auth | None:
        if not self._username and not self._password:
            return None
        return basic_auth(self._username, self._password)

    def connect(self) -> None:
        if not self._bolt_uri:
            raise ConnectionError("Empty bolt URI – check COGNODB/NEO4J/MEMGRAPH env vars")
        logger.info(
            "[%s] Connecting to %s (database=%s)",
            self.platform_name,
            self._bolt_uri,
            self._database or "<default>",
        )
        try:
            self._driver = GraphDatabase.driver(
                self._bolt_uri,
                auth=self._build_auth(),
                liveness_check_timeout=self._liveness_check_timeout_s,
                max_connection_pool_size=50,
                connection_acquisition_timeout=60.0,
                max_transaction_retry_time=30.0,
                keep_alive=True,
            )
            # verify_connectivity() raises ServiceUnavailable / AuthError
            self._driver.verify_connectivity()
        except AuthError as exc:
            self._safe_close_driver()
            raise ConnectionError(f"Auth failed for {self.platform_name}: {exc}") from exc
        except ServiceUnavailable as exc:
            self._safe_close_driver()
            raise ConnectionError(f"Unreachable {self.platform_name}: {exc}") from exc
        except Exception:
            # Do not let a partially opened driver survive a failed handshake.
            self._safe_close_driver()
            raise

        # Try to peek the version via the user's session info
        try:
            with self._session() as s:
                result = s.run("CALL dbms.components() YIELD name, versions, edition "
                               "RETURN name, versions, edition LIMIT 1")
                rec = result.single()
                if rec is not None:
                    versions = rec.get("versions") or ["Unknown"]
                    edition = rec.get("edition")
                    self._observed_version = (
                        f"{versions[0]} ({edition})" if edition else str(versions[0])
                    )
        except (Neo4jError, ClientError, DatabaseError):
            # Some cloud databases (Aura free tier, Memgraph community)
            # restrict dbms.*. Fall back silently.
            try:
                with self._session() as s:
                    rec = s.run(
                        "CALL db.labels() YIELD label RETURN count(*) AS c"
                    ).single()
                    if rec is not None:
                        self._observed_version = "Connected (version introspection blocked)"
            except Exception:
                self._observed_version = "Connected"

    def disconnect(self) -> None:
        self._safe_close_driver()

    def _safe_close_driver(self) -> None:
        driver = self._driver
        # Clear our reference first so a failed close cannot leave this adapter
        # reusing a connection with an exported PackStream buffer.
        self._driver = None
        if driver is not None:
            try:
                driver.close()
            except Exception:
                pass
            finally:
                del driver
                gc.collect()

    # ------------------------------------------------------------------
    # Session helpers
    # ------------------------------------------------------------------

    def _session(self) -> Session:
        if self._driver is None:
            raise RuntimeError("Adapter not connected – call connect() first")
        kwargs: dict[str, Any] = {}
        if self._database:
            kwargs["database"] = self._database
        return self._driver.session(**kwargs)

    def _execute_write_batch(self, cypher: str, rows: list[dict[str, Any]]) -> bool:
        """Execute one write batch with a fresh, fully-consumed session.

        A few Bolt endpoints have raised ``BufferError`` while a PackStream
        buffer from a failed request was still exported.  Session scoping and
        one new-driver retry release that buffer without hiding a persistent
        failure from the benchmark.

        Returns ``True`` when the one-time recovery path was used.
        """
        # Decouple the driver encoder from the CSV reader's dictionaries.
        payload = [dict(row) for row in rows]
        for attempt in range(2):
            try:
                with self._session() as session:
                    session.run(cypher, rows=payload).consume()
                return attempt == 1
            except BufferError:
                if attempt:
                    raise
                logger.warning(
                    "[%s] Bolt buffer export detected; reconnecting once and retrying batch",
                    self.platform_name,
                )
                self._safe_close_driver()
                self.connect()
        raise AssertionError("unreachable")

    # ------------------------------------------------------------------
    # Data management
    # ------------------------------------------------------------------

    def clear_data(self) -> None:
        caveats: list[str] = []
        # Drop in batches to avoid blowing transaction memory on large graphs
        with self._session() as s:
            while True:
                rec = s.run(self._clear_cypher_batch(), batch=50_000).single()
                deleted = rec["deleted"] if rec else 0
                if deleted == 0:
                    break
                logger.debug("[%s] clear_data deleted %d this batch", self.platform_name, deleted)

        # Best-effort drop indices + constraints so create_indices() is idempotent
        for stmt in self._drop_existing_indices_statements():
            try:
                with self._session() as s:
                    s.run(stmt).consume()
            except (ClientError, Neo4jError) as exc:
                # apoc missing on Memgraph/Aura free tier – ignore.
                caveats.append(f"drop-indices skipped: {exc.message}")
                logger.debug(
                    "[%s] Drop index skipped – %s", self.platform_name, exc.message
                )
        if caveats:
            logger.info("[%s] clear_data caveats: %s", self.platform_name, caveats)

    def create_indices(self) -> None:
        stmts = [
            self._create_vertex_id_unique_constraint(),
            self._create_vertex_id_index(),
            self._create_reputation_index(),
        ]
        with self._session() as s:
            for stmt in stmts:
                try:
                    s.run(stmt).consume()
                except (ClientError, Neo4jError) as exc:
                    logger.warning(
                        "[%s] create_indices ignored: %s",
                        self.platform_name,
                        exc.message,
                    )
        logger.info("[%s] Indices / constraints applied", self.platform_name)

    # ------------------------------------------------------------------
    # Ingestion
    # ------------------------------------------------------------------

    def ingest_batch(
        self,
        nodes: Iterable[dict[str, Any]],
        relationships: Iterable[dict[str, Any]],
        batch_size: int = 1000,
    ) -> IngestResult:
        caveats: list[str] = []
        t0 = time.perf_counter()
        node_count = 0
        rel_count = 0
        batches_processed = 0

        # ---- Nodes --------------------------------------------------------
        node_cypher = """
        UNWIND $rows AS row
        MERGE (u:User {id: row.id})
        SET u.type = row.type,
            u.status = row.status,
            u.reputation_score = toFloat(row.reputation_score),
            u.created_at = row.created_at
        """
        for chunk in _chunked(nodes, batch_size):
            try:
                if self._execute_write_batch(node_cypher, chunk):
                    caveats.append("Node batch recovered after Bolt BufferError")
            except TransientError as exc:
                caveats.append(f"Node-batch transient retry not implemented: {exc.message}")
                raise
            node_count += len(chunk)
            batches_processed += 1

        # ---- Relationships ------------------------------------------------
        rel_cypher = """
        UNWIND $rows AS row
        MATCH (s:User {id: row.source_id})
        MATCH (t:User {id: row.target_id})
        MERGE (s)-[r:EDGE_TYPE_HOLDER]->(t)
        """
        # Neo4j Cypher does not allow parameterising relationship types.
        # To support multiple types with a single plan we partition by
        # `rel_type` within each batch and emit a separate MERGE per type.
        for chunk in _chunked(relationships, batch_size):
            per_type: dict[str, list[dict[str, Any]]] = {}
            for r in chunk:
                per_type.setdefault(r.get("rel_type") or "RELATED", []).append(r)
            for rel_type, rows in per_type.items():
                sanitized = self._sanitize_rel_type(rel_type)
                stmt = (
                    "UNWIND $rows AS row "
                    "MATCH (s:User {id: row.source_id}) "
                    "MATCH (t:User {id: row.target_id}) "
                    f"MERGE (s)-[r:{sanitized}]->(t) "
                    "SET r.weight = toFloat(row.weight), "
                    "    r.timestamp = row.timestamp"
                )
                try:
                    if self._execute_write_batch(stmt, rows):
                        caveats.append("Relationship batch recovered after Bolt BufferError")
                except TransientError as exc:
                    caveats.append(f"Rel-batch transient: {exc.message}")
                    raise
            rel_count += len(chunk)
            batches_processed += 1

        total = time.perf_counter() - t0
        return IngestResult(
            total_time_sec=total,
            nodes_ingested=node_count,
            rels_ingested=rel_count,
            nodes_per_sec=(node_count / total if total > 0 else 0.0),
            rels_per_sec=(rel_count / total if total > 0 else 0.0),
            batches_processed=batches_processed,
            caveats=caveats,
        )

    @staticmethod
    def _sanitize_rel_type(raw: str) -> str:
        """Escape illegal chars in a user-supplied rel type name.

        Neo4j rel types are identifiers – backtick-wrap anything that
        contains non-alnum so the Cypher generated above won't parse error.
        """
        if raw.isidentifier():
            return raw
        escaped = raw.replace("`", "``")
        return f"`{escaped}`"

    # ------------------------------------------------------------------
    # Read primitives
    # ------------------------------------------------------------------

    def point_lookup(self, node_id: str) -> dict[str, Any] | None:
        with self._session() as s:
            rec = s.run(
                "MATCH (u:User {id: $id}) RETURN properties(u) AS props LIMIT 1",
                id=node_id,
            ).single()
            return dict(rec["props"]) if rec is not None and rec["props"] is not None else None

    def indexed_lookup(self, min_score: float) -> list[dict[str, Any]]:
        with self._session() as s:
            result = s.run(
                "MATCH (u:User) WHERE u.reputation_score > $min "
                "RETURN properties(u) AS props ORDER BY u.reputation_score DESC LIMIT 500",
                min=float(min_score),
            )
            return [dict(r["props"]) for r in result if r.get("props") is not None]

    def traverse_n_hop(
        self, start_node_id: str, hops: int
    ) -> list[dict[str, Any]]:
        if hops not in (1, 2, 3):
            raise ValueError(f"Unsupported hop count {hops}")
        with self._session() as s:
            rec = s.run(
                self._traversal_cypher(hops),
                start=start_node_id,
            ).single()
            return list(rec["neighbors"]) if rec is not None and rec.get("neighbors") else []

    def aggregate(self) -> dict[str, int]:
        with self._session() as s:
            result = s.run(
                "MATCH (u:User) WITH u.status AS status, count(*) AS cnt "
                "RETURN status, cnt"
            )
            return {
                str(r["status"]): int(r["cnt"])
                for r in result
                if r["status"] is not None
            }

    # ------------------------------------------------------------------
    # Write primitive
    # ------------------------------------------------------------------

    def write_query(
        self, source_id: str, target_id: str, rel_type: str = "FOLLOWS"
    ) -> bool:
        sanitized = self._sanitize_rel_type(rel_type)
        stmt = (
            "MATCH (s:User {id: $sid}) MATCH (t:User {id: $tid}) "
            f"MERGE (s)-[r:{sanitized}]->(t) "
            "ON CREATE SET r._inserted_at = datetime() "
            "RETURN r AS rel"
        )
        with self._session() as s:
            rec = s.run(stmt, sid=source_id, tid=target_id).single()
            return rec is not None

    # ------------------------------------------------------------------
    # Observability
    # ------------------------------------------------------------------

    def get_footprint(self) -> str:
        try:
            with self._session() as s:
                rec = s.run(
                    "CALL apoc.monitor.store() YIELD "
                    "logSize, stringSize, arraySize, relStoreSize, nodeStoreSize "
                    "RETURN logSize, stringSize, arraySize, relStoreSize, nodeStoreSize "
                    "LIMIT 1"
                ).single()
                if rec is None:
                    raise Neo4jError(message="apoc unavailable", code="N/A")
                total_kb = sum(
                    int(rec[k] or 0)
                    for k in (
                        "logSize",
                        "stringSize",
                        "arraySize",
                        "relStoreSize",
                        "nodeStoreSize",
                    )
                )
                return f"{total_kb:,.0f} KB (apoc.monitor.store)"
        except (Neo4jError, ClientError):
            # Fall back to counts-only footprint approximation
            try:
                with self._session() as s:
                    r = s.run(
                        "MATCH (n) WITH count(n) AS nc "
                        "OPTIONAL MATCH ()-[r]->() WITH nc, count(r) AS rc "
                        "RETURN nc, rc LIMIT 1"
                    ).single()
                    nc = int(r["nc"]) if r else 0
                    rc = int(r["rc"]) if r else 0
                    approx_kb = (nc * 128 + rc * 160) / 1024
                    return (
                        f"Not Observable (approx {approx_kb:,.0f} KB: "
                        f"{nc:,} nodes · {rc:,} rels)"
                    )
            except Exception:
                return "Not Observable"
