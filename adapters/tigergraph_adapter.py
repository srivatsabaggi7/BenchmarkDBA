"""
TigerGraph (Cloud) adapter.

Transport
---------
Uses the official ``pyTigerGraph`` v2 client (which wraps REST++ + GSQL
server endpoints) authenticated with a secret-derived API token.

Mapping of canonical benchmark primitives onto TigerGraph's data model:
  - Nodes.csv rows    -> Vertex type  ``User``   (id STRING PRIMARY KEY,
                                                type, status STRING,
                                                reputation_score DOUBLE,
                                                created_at STRING)
  - Rels.csv rows     -> Edge type    ``USER_EDGE`` (a generic
                                                directed edge with
                                                discriminator attribute
                                                ``rel_type`` so we can
                                                model arbitrary
                                                relationship types without
                                                GSQL DDL per-type; plus
                                                ``weight`` DOUBLE and
                                                ``timestamp`` STRING)
  - Indexes / lookup -> ``id`` is the PRIMARY KEY -> implicit global
                        index; ``reputation_score`` uses a secondary
                        VERTEX INDEX.
"""

from __future__ import annotations

import logging
import time
from itertools import islice
from typing import Any, Iterable, Iterator

from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from core.base_adapter import BaseGraphAdapter, IngestResult

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _chunked(iterable: Iterable[Any], size: int) -> Iterator[list[Any]]:
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            return
        yield chunk


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class TigerGraphAdapter(BaseGraphAdapter):
    """TigerGraph Cloud target via pyTigerGraph."""

    #: Vertex / Edge type names (hard-coded so GSQL and queries line up).
    VERTEX_TYPE = "User"
    EDGE_TYPE = "USER_EDGE"

    def __init__(
        self,
        host: str,
        username: str = "",
        password: str = "",
        graphname: str = "BenchmarkGraph",
        secret: str | None = None,
        gsql_port: int = 14240,
        restpp_port: int = 9000,
    ) -> None:
        self._host = host.rstrip("/")
        self._username = username
        self._password = password
        self._graphname = graphname
        self._secret = secret or None
        self._gsql_port = gsql_port
        self._restpp_port = restpp_port
        self._conn = None
        self._observed_version: str = "Unknown"

    # ------------------------------------------------------------------
    @property
    def platform_name(self) -> str:
        return "TigerGraph"

    @property
    def platform_version(self) -> str:
        return self._observed_version

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> None:
        if not self._host:
            raise ConnectionError("TG_HOST not configured – check .env")
        if not self._secret:
            raise ConnectionError("TG_SECRET not configured – check .env")
        try:
            import pyTigerGraph as tg  # local import for lazy dependency
        except ImportError as exc:  # pragma: no cover
            raise ConnectionError(
                "pyTigerGraph is not installed.  `pip install pyTigerGraph`."
            ) from exc

        logger.info(
            "[TigerGraph] connecting to %s graph=%s",
            self._host,
            self._graphname,
        )
        try:
            conn = tg.TigerGraphConnection(
                host=self._host,
                graphname=self._graphname,
            )
            token = conn.getToken(self._secret)[0]
            if not token:
                raise ConnectionError("getToken() returned no token")
            conn.apiToken = token
        except Exception as exc:
            raise ConnectionError(
                f"TigerGraph connect failed: {exc!r}"
            ) from exc

        self._conn = conn

        try:
            meta = conn.getVersion()
            if isinstance(meta, dict):
                self._observed_version = ", ".join(
                    f"{k} {v}" for k, v in list(meta.items())[:3]
                ) or "Unknown"
            elif meta is not None:
                self._observed_version = str(meta)
        except Exception:
            try:
                resp = conn.echo()
                self._observed_version = f"Connected (echo={resp})"
            except Exception:
                self._observed_version = "Connected"

        self._ensure_graph()

    def disconnect(self) -> None:
        try:
            if self._conn is not None:
                self._conn = None
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Schema helpers
    # ------------------------------------------------------------------

    def _ensure_graph(self) -> None:
        """Idempotently create the benchmark graph and schema objects."""
        assert self._conn is not None
        caveats: list[str] = []

        try:
            self._conn.createGraph(self._graphname, vertexTypes=[], edgeTypes=[],
                                   global_change=False, timeout=60_000)
        except Exception as exc:  # pragma: no cover
            caveats.append(f"createGraph (ignored): {exc!r}")

        # Switch our connection context onto this graph (required before
        # running graph-scoped endpoints)
        try:
            self._conn.graphname = self._graphname
        except Exception:
            pass

        # 2. Vertex type: User
        v_ddl = (
            f"CREATE VERTEX {self.VERTEX_TYPE} ("
            "PRIMARY_ID id STRING, "
            "type STRING, "
            "status STRING, "
            "reputation_score DOUBLE, "
            "created_at STRING"
            ") WITH STATS=\"OUTDEGREE_BY_EDGETYPE\""
        )
        try:
            self._conn.gsql(v_ddl)
        except Exception as exc:
            logger.warning(
                "[TigerGraph] GSQL schema creation requires password; "
                "assuming schema exists: %s",
                exc,
            )

        # 3. Edge type: USER_EDGE (directed, multi attributes)
        e_ddl = (
            f"CREATE DIRECTED EDGE {self.EDGE_TYPE} ("
            "FROM User TO User, "
            "rel_type STRING, "
            "weight DOUBLE, "
            "timestamp STRING"
            ")"
        )
        try:
            self._conn.gsql(e_ddl)
        except Exception as exc:
            logger.warning(
                "[TigerGraph] GSQL schema creation requires password; "
                "assuming schema exists: %s",
                exc,
            )

        # 4. Attach vertex + edge types to the graph (may be already attached)
        try:
            self._conn.gsql(
                f"USE GRAPH {self._graphname} "
                f"ALTER GRAPH {self._graphname} "
                f"ADD VERTEX {self.VERTEX_TYPE} "
                f"ADD EDGE {self.EDGE_TYPE}"
            )
        except Exception as exc:
            logger.warning(
                "[TigerGraph] GSQL schema creation requires password; "
                "assuming schema exists: %s",
                exc,
            )

        # 5. Publish schema / GPE rebuild (sync, blocks until ready)
        try:
            self._conn.gsql(
                f"USE GRAPH {self._graphname} "
                f"RUN GPE REBUILD OVERRIDE -graph {self._graphname}"
            )
        except Exception:
            # Community / free tier may not expose GPE REBUILD.
            # Publish via REST++ equivalent below.
            try:
                self._conn.gsql(
                    f"USE GRAPH {self._graphname} INSTALL QUERY EMPTY()"
                )
            except Exception as fallback_exc:
                logger.warning(
                    "[TigerGraph] GSQL schema publication skipped; "
                    "assuming schema exists: %s",
                    fallback_exc,
                )

        for c in caveats:
            logger.debug("[TigerGraph] schema ensure: %s", c)

    # ------------------------------------------------------------------
    # Data management
    # ------------------------------------------------------------------

    def clear_data(self) -> None:
        assert self._conn is not None
        caveats: list[str] = []
        try:
            self._conn.delEdges(
                sourceVertexType=self.VERTEX_TYPE,
                edgeType=self.EDGE_TYPE,
            )
        except Exception as exc:
            caveats.append(f"delEdges: {exc!r}")
        try:
            self._conn.delVertices(vertexType=self.VERTEX_TYPE)
        except Exception as exc:
            caveats.append(f"delVertices: {exc!r}")
        for c in caveats:
            logger.info("[TigerGraph] clear_data: %s", c)

    def create_indices(self) -> None:
        assert self._conn is not None
        # PRIMARY KEY(id) -> implicit vertex index already created.
        # Create a secondary index on reputation_score via TG DDL.
        ddl = (
            f"USE GRAPH {self._graphname} "
            f"CREATE INDEX idx_user_reputation ON "
            f"{self.VERTEX_TYPE}(reputation_score)"
        )
        try:
            self._conn.gsql(ddl)
        except Exception as exc:
            logger.warning(
                "[TigerGraph] GSQL schema creation requires password; "
                "assuming schema exists: %s",
                exc,
            )

    # ------------------------------------------------------------------
    # Ingestion
    # ------------------------------------------------------------------

    def ingest_batch(
        self,
        nodes: Iterable[dict[str, Any]],
        relationships: Iterable[dict[str, Any]],
        batch_size: int = 1000,
    ) -> IngestResult:
        assert self._conn is not None
        caveats: list[str] = []
        t0 = time.perf_counter()
        node_count = 0
        rel_count = 0
        batches_processed = 0

        # ---- Nodes --------------------------------------------------------
        for chunk in _chunked(nodes, batch_size):
            upserts = []
            for row in chunk:
                upserts.append(
                    {
                        "v_id": row["id"],
                        "v_type": self.VERTEX_TYPE,
                        "attributes": {
                            "type": str(row.get("type") or ""),
                            "status": str(row.get("status") or ""),
                            "reputation_score": float(
                                row.get("reputation_score") or 0.0
                            ),
                            "created_at": str(row.get("created_at") or ""),
                        },
                    }
                )
            try:
                self._upsert_vertex_batch(upserts)
            except Exception as exc:
                caveats.append(f"node upsert failed: {exc!r}")
                raise
            node_count += len(chunk)
            batches_processed += 1

        # ---- Relationships ------------------------------------------------
        for chunk in _chunked(relationships, batch_size):
            edges = []
            for row in chunk:
                edges.append(
                    {
                        "from_type": self.VERTEX_TYPE,
                        "from_id": row["source_id"],
                        "to_type": self.VERTEX_TYPE,
                        "to_id": row["target_id"],
                        "e_type": self.EDGE_TYPE,
                        "attributes": {
                            "rel_type": str(row.get("rel_type") or "RELATED"),
                            "weight": float(row.get("weight") or 0.0),
                            "timestamp": str(row.get("timestamp") or ""),
                        },
                    }
                )
            try:
                self._upsert_edge_batch(edges)
            except Exception as exc:
                caveats.append(f"edge upsert failed: {exc!r}")
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

    # ---- TG v2 upsert helpers ------------------------------------------------

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=0.5, min=0.2, max=4),
        retry=retry_if_exception_type((Exception,)),
        reraise=True,
    )
    def _upsert_vertex_batch(self, vertices: list[dict[str, Any]]) -> None:
        assert self._conn is not None
        self._conn.upsertVertices(vertices)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=0.5, min=0.2, max=4),
        retry=retry_if_exception_type((Exception,)),
        reraise=True,
    )
    def _upsert_edge_batch(self, edges: list[dict[str, Any]]) -> None:
        assert self._conn is not None
        self._conn.upsertEdges(edges)

    # ------------------------------------------------------------------
    # Read primitives
    # ------------------------------------------------------------------

    def point_lookup(self, node_id: str) -> dict[str, Any] | None:
        assert self._conn is not None
        try:
            data = self._conn.getVertices(
                vertexType=self.VERTEX_TYPE,
                vertexIds=node_id,
            )
        except Exception as exc:
            logger.debug("[TigerGraph] point_lookup error: %s", exc)
            return None
        if not data:
            return None
        # pyTigerGraph returns list[dict]; each dict has attributes nested.
        first = data[0]
        attrs = first.get("attributes") or {}
        return {
            "id": first.get("v_id") or node_id,
            **{k: v for k, v in attrs.items() if k != "id"},
        }

    def indexed_lookup(self, min_score: float) -> list[dict[str, Any]]:
        assert self._conn is not None
        # Build + run an interpreted GSQL query (no install required).
        query = (
            f"INTERPRET QUERY () FOR GRAPH {self._graphname} {{ "
            f"  SetAccum<VERTEX<{self.VERTEX_TYPE}>> @@bag;"
            f"  MinScore = {float(min_score):.6f};"
            f"  S = SELECT s "
            f"      FROM {self.VERTEX_TYPE}:s "
            f"      WHERE s.reputation_score > MinScore "
            f"      ORDER BY s.reputation_score DESC "
            f"      LIMIT 500;"
            f"  PRINT S [S.id AS id, S.type AS type, S.status AS status, "
            f"           S.reputation_score AS reputation_score, "
            f"           S.created_at AS created_at];"
            f"}}"
        )
        try:
            result = self._conn.runInterpretedQuery(query)
        except Exception as exc:
            logger.debug("[TigerGraph] indexed_lookup error: %s", exc)
            return []
        return self._extract_print_rows(result, "S")

    def traverse_n_hop(
        self, start_node_id: str, hops: int
    ) -> list[dict[str, Any]]:
        assert self._conn is not None
        if hops not in (1, 2, 3):
            raise ValueError(f"Unsupported hop count {hops}")
        query = (
            f"INTERPRET QUERY () FOR GRAPH {self._graphname} {{"
            f"  OrAccum<BOOL> @visited;"
            f"  SetAccum<VERTEX<{self.VERTEX_TYPE}>> @@neighbors;"
            f"  Start = {{\"{self._escape_quoted(start_node_id)}\"}};"
            f"  Neighbors = SELECT t "
            f"            FROM Start:s "
            f"                 -({self.EDGE_TYPE}*{hops})-> "
            f"                 {self.VERTEX_TYPE}:t "
            f"            ACCUM @@neighbors += t;"
            f"  PRINT @@neighbors;"
            f"}}"
        )
        try:
            result = self._conn.runInterpretedQuery(query)
        except Exception as exc:
            logger.debug("[TigerGraph] traverse_%d_hop error: %s", hops, exc)
            return []
        rows = self._extract_print_accum(result, "@@neighbors")
        # Dedup by id
        seen: set[str] = set()
        out: list[dict[str, Any]] = []
        for r in rows:
            key = str(r.get("id") or r.get("v_id") or "")
            if not key or key in seen:
                continue
            seen.add(key)
            out.append(r)
        return out

    def aggregate(self) -> dict[str, int]:
        assert self._conn is not None
        query = (
            f"INTERPRET QUERY () FOR GRAPH {self._graphname} {{"
            f"  MapAccum<STRING, INT> @@cnt;"
            f"  S = SELECT s FROM {self.VERTEX_TYPE}:s "
            f"      ACCUM @@cnt += (s.status -> 1);"
            f"  PRINT @@cnt;"
            f"}}"
        )
        try:
            result = self._conn.runInterpretedQuery(query)
        except Exception as exc:
            logger.debug("[TigerGraph] aggregate error: %s", exc)
            return {}
        out = self._extract_print_map(result, "@@cnt")
        return {str(k): int(v) for k, v in out.items()}

    # ------------------------------------------------------------------
    # Write primitive
    # ------------------------------------------------------------------

    def write_query(
        self, source_id: str, target_id: str, rel_type: str = "FOLLOWS"
    ) -> bool:
        assert self._conn is not None
        edge = [
            {
                "from_type": self.VERTEX_TYPE,
                "from_id": source_id,
                "to_type": self.VERTEX_TYPE,
                "to_id": target_id,
                "e_type": self.EDGE_TYPE,
                "attributes": {
                    "rel_type": rel_type,
                    "weight": 0.0,
                    "timestamp": str(int(time.time() * 1000)),
                },
            }
        ]
        try:
            self._conn.upsertEdges(edge)
            return True
        except Exception as exc:  # pragma: no cover
            logger.debug("[TigerGraph] write_query failed: %s", exc)
            return False

    # ------------------------------------------------------------------
    # Observability
    # ------------------------------------------------------------------

    def get_footprint(self) -> str:
        assert self._conn is not None
        try:
            size = self._conn.getGraphSize()
            if isinstance(size, dict):
                total = 0
                parts: list[str] = []
                for k, v in size.items():
                    try:
                        iv = int(v)
                    except (TypeError, ValueError):
                        iv = 0
                    total += iv
                    parts.append(f"{k}={v}")
                return f"GraphSize total={total:,} (" + ", ".join(parts[:4]) + ")"
            return f"GraphSize: {size}"
        except Exception:
            pass
        try:
            stats = self._conn.getStatistics()
            if isinstance(stats, dict):
                sample = ", ".join(
                    f"{k}={v}" for k, v in list(stats.items())[:5]
                )
                return f"Stats: {sample}"
            return "Not Observable"
        except Exception:
            return "Not Observable"

    # ------------------------------------------------------------------
    # Query-result extraction helpers (pyTigerGraph return shapes vary)
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_print_rows(
        result: Any, key: str
    ) -> list[dict[str, Any]]:
        """Return rows from PRINT S [...] result flatten attrs."""
        out: list[dict[str, Any]] = []
        if isinstance(result, dict):
            buckets = result.values()
        elif isinstance(result, list):
            buckets = result
        else:
            return []
        for bucket in buckets:
            if isinstance(bucket, dict):
                payload = bucket.get(key) or bucket
                if isinstance(payload, list):
                    for entry in payload:
                        if isinstance(entry, dict):
                            attr = entry.get("attributes") or entry
                            flat = {
                                **{k: v for k, v in attr.items() if not k.startswith("v_")},
                            }
                            if "v_id" in entry and "id" not in flat:
                                flat["id"] = entry["v_id"]
                            out.append(flat)
        return out

    @staticmethod
    def _extract_print_accum(result: Any, key: str) -> list[dict[str, Any]]:
        """Return SetAccum<VERTEX> contents as a list of attribute dicts."""
        out: list[dict[str, Any]] = []
        if isinstance(result, dict):
            buckets = list(result.values())
        elif isinstance(result, list):
            buckets = result
        else:
            return []
        for bucket in buckets:
            if not isinstance(bucket, dict):
                continue
            payload = bucket.get(key) or {}
            if isinstance(payload, dict):
                entries = payload.get("value") or payload.get("set") or []
            elif isinstance(payload, list):
                entries = payload
            else:
                entries = []
            for entry in entries:
                if isinstance(entry, dict):
                    attr = entry.get("attributes") or {}
                    flat = {
                        **{k: v for k, v in attr.items() if not k.startswith("v_")},
                    }
                    if "v_id" in entry and "id" not in flat:
                        flat["id"] = entry["v_id"]
                    out.append(flat)
        return out

    @staticmethod
    def _extract_print_map(result: Any, key: str) -> dict[str, int]:
        if isinstance(result, dict):
            buckets = list(result.values())
        elif isinstance(result, list):
            buckets = result
        else:
            return {}
        for bucket in buckets:
            if not isinstance(bucket, dict):
                continue
            payload = bucket.get(key) or {}
            if isinstance(payload, dict):
                inner = payload.get("value") or payload.get("map") or payload
                if isinstance(inner, dict):
                    return {str(k): int(v) for k, v in inner.items()}
        return {}

    @staticmethod
    def _escape_quoted(raw: str) -> str:
        return raw.replace("\\", "\\\\").replace('"', '\\"')
