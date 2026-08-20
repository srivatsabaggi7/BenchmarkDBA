"""
Base Adapter for Graph Database Benchmark Suite.

Defines the contract (ABC) that every concrete database adapter must
implement.  Both synchronous and asynchronous entrypoints are declared;
default async implementations delegate to the sync variants so adapters
only need to override the ones they can truly parallelise.
"""

from __future__ import annotations

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from typing import Any, Iterable

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Structured result containers
# ---------------------------------------------------------------------------


@dataclass
class IngestResult:
    """Reported back by :meth:`BaseGraphAdapter.ingest_batch`."""

    total_time_sec: float
    nodes_ingested: int
    rels_ingested: int
    nodes_per_sec: float
    rels_per_sec: float
    batches_processed: int
    caveats: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class BenchmarkMetrics:
    """Container for a single workload's latency measurements."""

    cold_latency_ms: float | None = None
    warm_latencies_ms: list[float] = field(default_factory=list)
    p50_ms: float | None = None
    p95_ms: float | None = None
    errors: int = 0
    caveats: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ConcurrencyResult:
    """Measured throughput at a specific client / read-write mix."""

    clients: int
    read_write_ratio: float
    duration_sec: float
    total_queries: int
    total_reads: int
    total_writes: int
    qps: float
    read_qps: float
    write_qps: float
    errors: int
    caveats: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ---------------------------------------------------------------------------
# Base adapter ABC
# ---------------------------------------------------------------------------


class BaseGraphAdapter(ABC):
    """Abstract contract every graph-DB adapter must satisfy.

    Subclasses are expected to be cheap to construct – the heavy
    work (authentication, session pooling) happens inside :meth:`connect`.
    """

    # ------------------------------------------------------------------
    # Metadata overrides
    # ------------------------------------------------------------------

    @property
    @abstractmethod
    def platform_name(self) -> str:
        """Human readable identifier (e.g. ``"Neo4j AuraDB"``)."""

    @property
    @abstractmethod
    def platform_version(self) -> str:
        """Version string if observable, otherwise ``"Unknown"``."""

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @abstractmethod
    def connect(self) -> None:
        """Authenticate and verify the connection.

        Implementations **MUST** raise ``ConnectionError`` (or a subclass)
        when the target cannot be reached, so the harness can record the
        failure under ``caveats`` and skip the rest of the benchmark for
        this platform.
        """

    @abstractmethod
    def disconnect(self) -> None:
        """Release connection pools / close HTTP sessions."""

    def __enter__(self) -> "BaseGraphAdapter":
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.disconnect()

    async def __aenter__(self) -> "BaseGraphAdapter":
        await self.connect_async()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.disconnect_async()

    # ------------------------------------------------------------------
    # Async-friendly wrappers (delegate to sync by default)
    # ------------------------------------------------------------------

    async def connect_async(self) -> None:
        await asyncio.to_thread(self.connect)

    async def disconnect_async(self) -> None:
        await asyncio.to_thread(self.disconnect)

    # ------------------------------------------------------------------
    # Data management
    # ------------------------------------------------------------------

    @abstractmethod
    def clear_data(self) -> None:
        """Wipe every vertex/edge in the target graph.

        Used before every benchmark run so we start from a known empty
        state.  Must never drop the graph/database itself – only the
        contents.
        """

    @abstractmethod
    def create_indices(self) -> None:
        """Create the benchmark's canonical indices.

        Mandatory indices (expressed as property-graph idioms):
          - On vertices labelled ``User`` / attribute ``id``   (unique)
          - On vertices labelled ``User`` / attribute ``reputation_score``
        """

    async def clear_data_async(self) -> None:
        await asyncio.to_thread(self.clear_data)

    async def create_indices_async(self) -> None:
        await asyncio.to_thread(self.create_indices)

    # ------------------------------------------------------------------
    # Bulk ingestion
    # ------------------------------------------------------------------

    @abstractmethod
    def ingest_batch(
        self,
        nodes: Iterable[dict[str, Any]],
        relationships: Iterable[dict[str, Any]],
        batch_size: int = 1000,
    ) -> IngestResult:
        """Load ``nodes`` and ``relationships`` in fixed-size batches.

        Parameters
        ----------
        nodes:
            Row-dicts with keys matching ``nodes.csv``:
            ``id, type, status, reputation_score, created_at``.
        relationships:
            Row-dicts with keys matching ``relationships.csv``:
            ``source_id, target_id, rel_type, weight, timestamp``.
        batch_size:
            Maximum number of records sent per backend transaction.
        """

    async def ingest_batch_async(
        self,
        nodes: Iterable[dict[str, Any]],
        relationships: Iterable[dict[str, Any]],
        batch_size: int = 1000,
    ) -> IngestResult:
        return await asyncio.to_thread(
            self.ingest_batch, nodes, relationships, batch_size
        )

    # ------------------------------------------------------------------
    # Read workload primitives
    # ------------------------------------------------------------------

    @abstractmethod
    def point_lookup(self, node_id: str) -> dict[str, Any] | None:
        """Retrieve a single vertex by ``id``."""

    @abstractmethod
    def indexed_lookup(self, min_score: float) -> list[dict[str, Any]]:
        """Return vertices with ``reputation_score > min_score``."""

    @abstractmethod
    def traverse_n_hop(
        self, start_node_id: str, hops: int
    ) -> list[dict[str, Any]]:
        """Return the distinct neighbor set reachable in ``hops`` edges.

        ``hops`` is always 1, 2, or 3.  Implementations should walk the
        graph bidirectionally or use the native multi-hop primitive.
        """

    @abstractmethod
    def aggregate(self) -> dict[str, int]:
        """Group vertices by ``status`` and return ``{status: count}``."""

    # ---- async variants ------------------------------------------------

    async def point_lookup_async(self, node_id: str) -> dict[str, Any] | None:
        return await asyncio.to_thread(self.point_lookup, node_id)

    async def indexed_lookup_async(
        self, min_score: float
    ) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self.indexed_lookup, min_score)

    async def traverse_n_hop_async(
        self, start_node_id: str, hops: int
    ) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self.traverse_n_hop, start_node_id, hops)

    async def aggregate_async(self) -> dict[str, int]:
        return await asyncio.to_thread(self.aggregate)

    # ------------------------------------------------------------------
    # Write workload primitive
    # ------------------------------------------------------------------

    @abstractmethod
    def write_query(
        self, source_id: str, target_id: str, rel_type: str = "FOLLOWS"
    ) -> bool:
        """Insert a new edge (and its endpoints if missing).

        Returns ``True`` on successful insertion, ``False`` if the
        operation was a no-op (e.g. duplicate edge ignored).
        """

    async def write_query_async(
        self, source_id: str, target_id: str, rel_type: str = "FOLLOWS"
    ) -> bool:
        return await asyncio.to_thread(
            self.write_query, source_id, target_id, rel_type
        )

    # ------------------------------------------------------------------
    # Observability
    # ------------------------------------------------------------------

    @abstractmethod
    def get_footprint(self) -> str | dict[str, Any]:
        """Return footprint data, or ``"Not Observable"`` when unavailable.

        Structured values use ``storage_mb`` and/or ``memory_mb`` plus a
        ``notes`` field so callers never mistake a proxy for a byte-size
        measurement.
        """

    # ------------------------------------------------------------------
    # Shared helpers (harness-facing utility wrappers)
    # ------------------------------------------------------------------

    @staticmethod
    def _timed_ms(fn, *args, **kwargs) -> tuple[Any, float]:
        """Execute ``fn`` and return ``(result, wall_time_ms)``."""
        t0 = time.perf_counter()
        result = fn(*args, **kwargs)
        dt_ms = (time.perf_counter() - t0) * 1000.0
        return result, dt_ms
