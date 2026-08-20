"""FalkorDB adapter using the native OpenCypher graph API."""

from __future__ import annotations

import logging
import time
from itertools import islice
from typing import Any, Iterable, Iterator

from falkordb import FalkorDB

from core.base_adapter import BaseGraphAdapter, IngestResult

logger = logging.getLogger(__name__)


def _chunked(iterable: Iterable[Any], size: int) -> Iterator[list[Any]]:
    if size <= 0:
        raise ValueError("batch_size must be greater than zero")
    iterator = iter(iterable)
    while True:
        chunk = list(islice(iterator, size))
        if not chunk:
            return
        yield chunk


class FalkorDBAdapter(BaseGraphAdapter):
    """FalkorDB target through ``FalkorDB(host, port).select_graph()``."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        graphname: str = "BenchmarkGraph",
    ) -> None:
        self.host = host
        self.port = port
        self.graphname = graphname
        self._db: FalkorDB | None = None
        self.graph = None
        self._observed_version = "Connected"

    @property
    def platform_name(self) -> str:
        return "FalkorDB"

    @property
    def platform_version(self) -> str:
        return self._observed_version

    def connect(self) -> None:
        if not self.host:
            raise ConnectionError("FALKORDB_HOST not configured")
        try:
            self._db = FalkorDB(host=self.host, port=self.port)
            self.graph = self._db.select_graph(self.graphname)
            self.graph.query("RETURN 1 AS connected")
        except Exception as exc:
            self.disconnect()
            raise ConnectionError(f"FalkorDB connect failed: {exc!r}") from exc

    def disconnect(self) -> None:
        self.graph = None
        if self._db is not None:
            close = getattr(self._db, "close", None)
            if callable(close):
                try:
                    close()
                except Exception:
                    pass
            self._db = None

    def _require_graph(self):
        if self.graph is None:
            raise RuntimeError("FalkorDBAdapter not connected")
        return self.graph

    def _query(self, cypher: str, params: dict[str, Any] | None = None):
        graph = self._require_graph()
        if params is None:
            return graph.query(cypher)
        return graph.query(cypher, params=params)

    @staticmethod
    def _rows(result: Any) -> list[Any]:
        rows = getattr(result, "result_set", result)
        return list(rows) if rows is not None else []

    @staticmethod
    def _row_value(row: Any, index: int = 0, key: str | None = None) -> Any:
        if isinstance(row, dict) and key is not None:
            return row.get(key)
        if isinstance(row, (list, tuple)) and len(row) > index:
            return row[index]
        return row

    @staticmethod
    def _entity_to_dict(entity: Any) -> dict[str, Any]:
        """Convert a FalkorDB Node/Edge result to its properties dict."""
        properties = getattr(entity, "properties", None)
        if isinstance(properties, dict):
            return dict(properties)
        if isinstance(entity, dict):
            return dict(entity)
        return entity

    @staticmethod
    def _escape(value: Any) -> str:
        return str(value).replace("\\", "\\\\").replace("'", "\\'")

    def clear_data(self) -> None:
        while True:
            rows = self._rows(
                self._query(
                    "MATCH (n) WITH n LIMIT $limit DETACH DELETE n "
                    "RETURN count(n) AS deleted",
                    {"limit": 1000},
                )
            )
            deleted = self._row_value(rows[0], key="deleted") if rows else 0
            if not deleted:
                return

    def create_indices(self) -> None:
        for query in (
            "CREATE INDEX FOR (u:User) ON (u.id)",
            "CREATE INDEX FOR (u:User) ON (u.reputation_score)",
        ):
            try:
                self._query(query)
            except Exception as exc:
                logger.warning("[FalkorDB] index creation skipped: %s", exc)

    def ingest_batch(
        self,
        nodes: Iterable[dict[str, Any]],
        relationships: Iterable[dict[str, Any]],
        batch_size: int = 1000,
    ) -> IngestResult:
        start = time.perf_counter()
        nodes_ingested = 0
        rels_ingested = 0
        batches = 0

        for chunk in _chunked(nodes, batch_size):
            rows = [
                {
                    "id": str(row["id"]),
                    "type": str(row.get("type") or ""),
                    "status": str(row.get("status") or ""),
                    "reputation_score": float(row.get("reputation_score") or 0),
                    "created_at": str(row.get("created_at") or ""),
                }
                for row in chunk
            ]
            self._query(
                "UNWIND $rows AS row "
                "MERGE (u:User {id: row.id}) "
                "SET u.type = row.type, u.status = row.status, "
                "u.reputation_score = row.reputation_score, "
                "u.created_at = row.created_at",
                {"rows": rows},
            )
            nodes_ingested += len(chunk)
            batches += 1

        for chunk in _chunked(relationships, batch_size):
            rows = [
                {
                    "source_id": str(row["source_id"]),
                    "target_id": str(row["target_id"]),
                    "rel_type": str(row.get("rel_type") or "RELATED"),
                    "weight": float(row.get("weight") or 0),
                    "timestamp": str(row.get("timestamp") or ""),
                }
                for row in chunk
            ]
            self._query(
                "UNWIND $rows AS row "
                "MATCH (source:User {id: row.source_id}), "
                "(target:User {id: row.target_id}) "
                "CREATE (source)-[r:USER_EDGE]->(target) "
                "SET r.rel_type = row.rel_type, r.weight = row.weight, "
                "r.timestamp = row.timestamp",
                {"rows": rows},
            )
            rels_ingested += len(chunk)
            batches += 1

        elapsed = time.perf_counter() - start
        return IngestResult(
            total_time_sec=elapsed,
            nodes_ingested=nodes_ingested,
            rels_ingested=rels_ingested,
            nodes_per_sec=nodes_ingested / elapsed if elapsed else 0,
            rels_per_sec=rels_ingested / elapsed if elapsed else 0,
            batches_processed=batches,
        )

    def point_lookup(self, node_id: str) -> dict[str, Any] | None:
        rows = self._rows(
            self._query(
                "MATCH (u:User {id: $id}) RETURN u LIMIT 1", {"id": node_id}
            )
        )
        if not rows:
            return None
        value = self._row_value(rows[0])
        return self._entity_to_dict(value)

    def indexed_lookup(self, min_score: float) -> list[dict[str, Any]]:
        rows = self._rows(
            self._query(
                "MATCH (u:User) WHERE u.reputation_score > $score "
                "RETURN u ORDER BY u.reputation_score DESC LIMIT 500",
                {"score": min_score},
            )
        )
        return [self._entity_to_dict(self._row_value(row)) for row in rows]

    def traverse_n_hop(self, start_node_id: str, hops: int) -> list[dict[str, Any]]:
        if hops not in (1, 2, 3):
            raise ValueError(f"Unsupported hop count {hops}")
        rows = self._rows(
            self._query(
                f"MATCH (start:User {{id: $id}})-[*1..{hops}]-(neighbor:User) "
                "RETURN DISTINCT neighbor LIMIT 10000",
                {"id": start_node_id},
            )
        )
        return [self._entity_to_dict(self._row_value(row)) for row in rows]

    def aggregate(self) -> dict[str, int]:
        rows = self._rows(
            self._query(
                "MATCH (u:User) RETURN u.status AS status, count(u) AS count"
            )
        )
        grouped: dict[str, int] = {}
        for row in rows:
            status = self._row_value(row, index=0, key="status")
            count = self._row_value(row, index=1, key="count")
            grouped[str(status)] = int(count)
        return grouped

    def write_query(self, source_id: str, target_id: str, rel_type: str = "FOLLOWS") -> bool:
        self._query(
            "MATCH (source:User {id: $source}), (target:User {id: $target}) "
            "CREATE (source)-[r:USER_EDGE {rel_type: $rel_type}]->(target)",
            {"source": source_id, "target": target_id, "rel_type": rel_type},
        )
        return True

    def get_footprint(self) -> str | dict[str, Any]:
        """Capture FalkorDB's Redis-reported resident memory usage.

        Redis exposes memory directly through ``INFO memory``.  This is a
        runtime-memory observation, not an on-disk storage measurement.
        """
        try:
            if self._db is None:
                return "Not Observable"
            try:
                info = self._db.info("memory")
            except TypeError:
                # Older client versions expose only the unsectioned INFO API.
                info = self._db.info()
            if not isinstance(info, dict):
                return "Not Observable"

            used_bytes = info.get("used_memory")
            if used_bytes is not None:
                return {
                    "storage_mb": None,
                    "memory_mb": round(int(used_bytes) / (1024 * 1024), 2),
                    "memory_human": info.get("used_memory_human"),
                    "notes": (
                        "Redis INFO memory queried through the FalkorDB "
                        "client; this is resident memory, not disk storage."
                    ),
                }
        except Exception as exc:
            logger.warning("[FalkorDB] footprint observation failed: %s", exc)
        return "Not Observable"
