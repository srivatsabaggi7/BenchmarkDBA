"""
ArcadeDB (Docker, HTTP REST) adapter.

Transport
---------
Uses ArcadeDB's native HTTP REST API via ``httpx`` with HTTP Basic auth.
ArcadeDB exposes ``/api/v1/command/<db>`` for any query language +
``/api/v1/batch/<db>`` for multi-statement transaction batches which we
use for ingestion and clear_data.

Language selection
------------------
We send Gremlin/Groovy because ArcadeDB guarantees that Gremlin works on
every deployment (REST + embedded + Postgres protocol) and has broad
coverage for indexing/DML operations via graph API.  Cypher via the
Cypher plugin is an alternative but requires the server to have loaded
the ``cypher-server`` plugin, which we cannot assume.

Data model
----------
  - Vertex label   ``User``      with properties:
        id (String, unique index), type, status,
        reputation_score (Double), created_at (String)
  - Edge labels    ``rel_type`` (per value of rel_type in the dataset,
        e.g. FOLLOWS, TRUSTS, etc.) between (User, User) with
        properties weight (Double) and timestamp (String).
"""

from __future__ import annotations

import base64
import logging
import time
from itertools import islice
from typing import Any, Iterable, Iterator

import httpx

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


def _basic_auth_header(username: str, password: str) -> str:
    token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode()
    return f"Basic {token}"


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class ArcadeDBAdapter(BaseGraphAdapter):
    """ArcadeDB target over native HTTP REST (port 2480 by default)."""

    VERTEX_LABEL = "User"
    QUERY_LANG = "gremlin"

    def __init__(
        self,
        host: str,
        http_port: int,
        graph_name: str,
        username: str,
        password: str,
        timeout_seconds: float = 30.0,
    ) -> None:
        host = host.rstrip("/")
        # ArcadeDB server REST root
        self._base = f"{host}:{http_port}"
        self._graph_name = graph_name
        self._username = username
        self._password = password
        self._timeout = timeout_seconds
        self._client: httpx.Client | None = None
        self._observed_version: str = "Unknown"

    # ------------------------------------------------------------------
    @property
    def platform_name(self) -> str:
        return "ArcadeDB"

    @property
    def platform_version(self) -> str:
        return self._observed_version

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> None:
        if not self._base:
            raise ConnectionError("ARCADEDB_HOST/HTTP_PORT not configured")
        headers = {
            "Authorization": _basic_auth_header(self._username, self._password),
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        self._client = httpx.Client(
            headers=headers,
            timeout=httpx.Timeout(self._timeout),
        )

        # 1. Server alive + version fingerprint
        try:
            resp = self._client.get(f"{self._base}/api/v1/server")
            resp.raise_for_status()
            body = resp.json()
        except (httpx.HTTPError, ValueError) as exc:
            self.disconnect()
            raise ConnectionError(f"ArcadeDB server unreachable: {exc!r}") from exc

        version = None
        if isinstance(body, dict):
            for key in ("version", "releaseName", "name"):
                if body.get(key):
                    version = body[key]
                    break
        self._observed_version = version or "Connected"

        # 2. Ensure target database exists (Docker instances typically
        #    boot with only the default ``graph`` db).
        databases = body.get("databases", []) if isinstance(body, dict) else []
        if self._graph_name not in databases:
            try:
                self._client.post(
                    f"{self._base}/api/v1/server",
                    json={
                        "language": "sql",
                        "command": "create database",
                        "databaseName": self._graph_name,
                    },
                )
            except httpx.HTTPError as exc:
                logger.warning(
                    "[ArcadeDB] cannot create database %s: %s",
                    self._graph_name,
                    exc,
                )

        # 3. Auth smoke test against the target graph
        try:
            self._run_command(
                self._graph_name, self.QUERY_LANG, "g.V().limit(1).count()"
            )
        except Exception as exc:
            self.disconnect()
            raise ConnectionError(
                f"ArcadeDB auth failed for graph '{self._graph_name}': {exc!r}"
            ) from exc

    def disconnect(self) -> None:
        if self._client is not None:
            try:
                self._client.close()
            finally:
                self._client = None

    # ------------------------------------------------------------------
    # Low-level REST wrappers
    # ------------------------------------------------------------------

    def _require_client(self) -> httpx.Client:
        if self._client is None:
            raise RuntimeError("ArcadeDBAdapter not connected")
        return self._client

    def _run_command(
        self, database: str, language: str, command: str, params: dict | None = None
    ) -> list[dict[str, Any]]:
        """Execute a single ``command`` against ``database``.

        Returns the list of result rows from ArcadeDB /command endpoint.
        ArcadeDB returns ``{"result": [...]}`` shaped payloads for
        Gremlin queries.
        """
        client = self._require_client()
        payload: dict[str, Any] = {
            "language": language,
            "command": command,
        }
        if params:
            payload["params"] = params
        url = f"{self._base}/api/v1/command/{database}"
        resp = client.post(url, json=payload)
        resp.raise_for_status()
        body = resp.json()
        if isinstance(body, dict) and "result" in body and isinstance(body["result"], list):
            return body["result"]
        if isinstance(body, list):
            return body
        return []

    def _run_batch(
        self, database: str, language: str, commands: list[str]
    ) -> list[Any]:
        """Run SQLScript statements as one command request."""
        if not commands:
            return []
        client = self._require_client()
        payload = {
            "language": "sqlscript",
            "command": ";".join(commands),
        }
        url = f"{self._base}/api/v1/command/{database}"
        resp = client.post(url, json=payload)
        resp.raise_for_status()
        body = resp.json()
        if isinstance(body, list):
            return body
        if isinstance(body, dict) and "results" in body:
            return body["results"]
        return []

    # ------------------------------------------------------------------
    # Data management
    # ------------------------------------------------------------------

    def clear_data(self) -> None:
        # Drop vertices in batches because ArcadeDB single-tx size limits
        # on Docker instances can be quite low.
        caveats: list[str] = []
        while True:
            try:
                rows = self._run_command(
                    self._graph_name,
                    self.QUERY_LANG,
                    "g.V().limit(1000).drop().iterate(); g.V().limit(1).count()",
                )
                remaining = (rows[-1] if rows else 0)
            except (httpx.HTTPError, ValueError) as exc:
                caveats.append(f"clear_data batch error: {exc!r}")
                break
            try:
                remaining_int = int(remaining)
            except (TypeError, ValueError):
                remaining_int = 0
            if remaining_int == 0:
                break
        for c in caveats:
            logger.info("[ArcadeDB] clear_data: %s", c)

    def create_indices(self) -> None:
        # 1. Unique index on User.id (ArcadeDB Schema API via command)
        cmds = [
            # Make sure the schema types exist before indexing
            (
                "CREATE VERTEX TYPE User IF NOT EXISTS"
            ),
            (
                "CREATE PROPERTY User.id IF NOT EXISTS STRING"
            ),
            (
                "CREATE PROPERTY User.reputation_score IF NOT EXISTS DOUBLE"
            ),
            (
                "CREATE PROPERTY User.type IF NOT EXISTS STRING"
            ),
            (
                "CREATE PROPERTY User.status IF NOT EXISTS STRING"
            ),
            (
                "CREATE PROPERTY User.created_at IF NOT EXISTS STRING"
            ),
            (
                "CREATE INDEX User_id IF NOT EXISTS ON User (id) UNIQUE"
            ),
            (
                "CREATE INDEX User_reputation_score IF NOT EXISTS "
                "ON User (reputation_score) NOTUNIQUE"
            ),
        ]
        for cmd in cmds:
            try:
                # Schema commands use ArcadeDB's sql language, not Gremlin
                self._run_command(self._graph_name, "sql", cmd)
            except (httpx.HTTPError, ValueError) as exc:
                logger.warning("[ArcadeDB] index/schema skipped: %s", exc)

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

        # ---- Vertices -----------------------------------------------------
        for chunk in _chunked(nodes, batch_size):
            statements: list[str] = []
            for row in chunk:
                statements.append(
                    "INSERT INTO User SET "
                    f"id = '{self._sqlstr(row['id'])}', "
                    f"type = '{self._sqlstr(row.get('type') or '')}', "
                    f"status = '{self._sqlstr(row.get('status') or '')}', "
                    f"reputation_score = {float(row.get('reputation_score') or 0.0)}, "
                    f"created_at = '{self._sqlstr(row.get('created_at') or '')}'"
                )
            try:
                self._run_batch(self._graph_name, "sqlscript", statements)
            except (httpx.HTTPError, ValueError) as exc:
                caveats.append(f"node batch failed: {exc!r}")
                raise
            node_count += len(chunk)
            batches_processed += 1

        # ---- Edges --------------------------------------------------------
        for chunk in _chunked(relationships, batch_size):
            statements = []
            for row in chunk:
                rel_type = self._sanitize_label(row.get("rel_type") or "RELATED")
                sid = self._sqlstr(row["source_id"])
                tid = self._sqlstr(row["target_id"])
                weight = float(row.get("weight") or 0.0)
                ts = self._sqlstr(row.get("timestamp") or "")
                statements.append(
                    f"CREATE EDGE {rel_type} FROM "
                    f"(SELECT FROM User WHERE id = '{sid}') TO "
                    f"(SELECT FROM User WHERE id = '{tid}') SET "
                    f"weight = {weight}, timestamp = '{ts}'"
                )
            try:
                self._run_batch(self._graph_name, "sqlscript", statements)
            except (httpx.HTTPError, ValueError) as exc:
                caveats.append(f"edge batch failed: {exc!r}")
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

    # ------------------------------------------------------------------
    # Read primitives
    # ------------------------------------------------------------------

    def point_lookup(self, node_id: str) -> dict[str, Any] | None:
        query = (
            "g.V().has('User','id','"
            + self._gstr(node_id)
            + "').limit(1)"
            + ".valueMap(true).unfold()"
        )
        try:
            rows = self._run_command(self._graph_name, self.QUERY_LANG, query)
        except (httpx.HTTPError, ValueError):
            return None
        out = self._flatten_value_map_result(rows)
        return out[0] if out else None

    def indexed_lookup(self, min_score: float) -> list[dict[str, Any]]:
        query = (
            "g.V().hasLabel('User')"
            f".has('reputation_score',gt({float(min_score):.8f}))"
            ".order().by('reputation_score',decr).limit(500)"
            ".valueMap(true)"
        )
        try:
            rows = self._run_command(self._graph_name, self.QUERY_LANG, query)
        except (httpx.HTTPError, ValueError):
            return []
        return self._flatten_value_map_result(rows)

    def traverse_n_hop(
        self, start_node_id: str, hops: int
    ) -> list[dict[str, Any]]:
        if hops not in (1, 2, 3):
            raise ValueError(f"Unsupported hop count {hops}")
        query = (
            "g.V().has('User','id','" + self._gstr(start_node_id) + "')"
            f".repeat(bothE().otherV().simplePath()).times({hops})"
            ".emit().dedup()"
            ".limit(10000)"
            ".valueMap(true)"
        )
        try:
            rows = self._run_command(self._graph_name, self.QUERY_LANG, query)
        except (httpx.HTTPError, ValueError):
            return []
        return self._flatten_value_map_result(rows)

    def aggregate(self) -> dict[str, int]:
        query = (
            "g.V().hasLabel('User')"
            ".groupCount().by('status')"
        )
        try:
            rows = self._run_command(self._graph_name, self.QUERY_LANG, query)
        except (httpx.HTTPError, ValueError):
            return {}
        # Result shape: list[0] = {status1: count, status2: count, ...}
        if not rows:
            return {}
        first = rows[0]
        if isinstance(first, dict):
            return {str(k): int(v) for k, v in first.items() if v is not None}
        return {}

    # ------------------------------------------------------------------
    # Write primitive
    # ------------------------------------------------------------------

    def write_query(
        self, source_id: str, target_id: str, rel_type: str = "FOLLOWS"
    ) -> bool:
        label = self._sanitize_label(rel_type)
        query = (
            "g.V().has('User','id','" + self._gstr(source_id) + "').as('s')"
            ".V().has('User','id','" + self._gstr(target_id) + "').as('t')"
            f".coalesce(inE('{label}').where(outV().as('s')), "
            f"  addE('{label}').from('s').to('t')"
            f".property('_inserted_at',{int(time.time() * 1000)}))"
            ".count()"
        )
        try:
            rows = self._run_command(self._graph_name, self.QUERY_LANG, query)
            if not rows:
                return False
            first = rows[0]
            try:
                return int(first) > 0
            except (TypeError, ValueError):
                return True
        except (httpx.HTTPError, ValueError):
            return False

    # ------------------------------------------------------------------
    # Observability
    # ------------------------------------------------------------------

    def get_footprint(self) -> str:
        # Try SQL schema / storage info endpoint (ArcadeDB exposes this as
        # ``SELECT ... FROM stats``-ish queries via the sql language).
        try:
            rows = self._run_command(
                self._graph_name,
                "sql",
                "SELECT FROM stats WHERE NAME LIKE 'size%' LIMIT 10",
            )
            if rows:
                parts = []
                for r in rows:
                    if isinstance(r, dict):
                        for k, v in list(r.items())[:3]:
                            parts.append(f"{k}={v}")
                return "stats: " + ", ".join(parts[:8])
        except Exception:
            return "Not Observable"
        # Count-based approximation
        try:
            vc = self._run_command(
                self._graph_name, self.QUERY_LANG, "g.V().count()"
            )
            ec = self._run_command(
                self._graph_name, self.QUERY_LANG, "g.E().count()"
            )
            vi = int(vc[-1]) if vc else 0
            ei = int(ec[-1]) if ec else 0
            approx_kb = (vi * 128 + ei * 192) / 1024
            return (
                f"Not Observable (approx {approx_kb:,.0f} KB: "
                f"{vi:,} vertices · {ei:,} edges)"
            )
        except Exception:
            return "Not Observable"

    # ------------------------------------------------------------------
    # Utility: Gremlin string escaping / label sanitizers
    # ------------------------------------------------------------------

    @staticmethod
    def _gstr(raw: Any) -> str:
        s = str(raw)
        return s.replace("\\", "\\\\").replace("'", "\\'")

    @staticmethod
    def _sqlstr(raw: Any) -> str:
        return str(raw).replace("'", "''")

    @staticmethod
    def _sanitize_label(raw: str) -> str:
        if raw and all(c.isalnum() or c == "_" for c in raw):
            return raw
        escaped = "".join(c if c.isalnum() or c == "_" else "_" for c in raw)
        return escaped or "RELATED"

    @staticmethod
    def _props_to_gremlin_map(**kwargs: Any) -> str:
        out: list[str] = []
        for k, v in kwargs.items():
            if isinstance(v, float):
                out.append(f"'{k}',{v}")
            else:
                out.append(f"'{k}','{ArcadeDBAdapter._gstr(v)}'")
        return ",".join(out)

    # ------------------------------------------------------------------
    # Utility: flatten ``valueMap(true)`` Gremlin results
    # ------------------------------------------------------------------

    @staticmethod
    def _flatten_value_map_result(rows: list[Any]) -> list[dict[str, Any]]:
        """ArcadeDB Gremlin returns valueMap(true) rows as:

            list[ dict[label:T, id:#N:1, prop_key:[val], ...] ]

        where each property is wrapped in a 1-element list (TinkerPop
        semantics).  This helper unpacks them into plain dicts with
        scalar values.
        """
        flat: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            record: dict[str, Any] = {}
            for key, value in row.items():
                # Gremlin ``T.label`` / ``T.id`` can be object keys in
                # some drivers; skip the T tokens and use string keys
                tname = getattr(key, "name", None) or str(key)
                if tname.lower() in ("label", "id") and isinstance(value, (str, int)):
                    record[tname.lower()] = value
                    continue
                if isinstance(value, list) and len(value) == 1:
                    record[key] = value[0]
                else:
                    record[key] = value
            # Ensure 'id' key exists (prefer the property over T.id)
            if "id" not in record and "ID" in record:
                record["id"] = record.pop("ID")
            flat.append(record)
        return flat
