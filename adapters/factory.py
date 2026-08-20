"""
Adapter Factory (Factory Pattern).

Central registry that resolves a platform name (string) into a fully
configured :class:`BaseGraphAdapter` instance.  Adapters are lazily
imported so a missing optional dependency (e.g. ``pyTigerGraph``) will
not crash the harness for the other platforms.
"""

from __future__ import annotations

import logging
import os
from typing import Callable

from core.base_adapter import BaseGraphAdapter

logger = logging.getLogger(__name__)

#: Canonical list of platforms the suite compares – in display order.
SUPPORTED_PLATFORMS: tuple[str, ...] = (
    "CognoDB",
    "Neo4j",
    "Memgraph",
    "TigerGraph",
    "ArcadeDB",
    "FalkorDB",
)


class AdapterFactory:
    """Construct a configured adapter for a named platform."""

    # populated lazily – see :meth:`_construct`
    _registry: dict[str, Callable[[], BaseGraphAdapter]] = {}

    @classmethod
    def available_platforms(cls) -> list[str]:
        return list(SUPPORTED_PLATFORMS)

    # ------------------------------------------------------------------
    @classmethod
    def build(cls, platform: str) -> BaseGraphAdapter:
        """Return a **new**, disconnected adapter for ``platform``.

        The caller is responsible for calling :meth:`BaseGraphAdapter.connect`
        (or using the context manager) before issuing queries.
        """
        platform = platform.strip().lower()
        constructors = {
            "cognodb": cls._build_cognodb,
            "neo4j": cls._build_neo4j,
            "memgraph": cls._build_memgraph,
            "tigergraph": cls._build_tigergraph,
            "arcadedb": cls._build_arcadedb,
            "falkordb": cls._build_falkordb,
        }
        if platform not in constructors:
            raise ValueError(
                f"Unknown platform '{platform}'. "
                f"Choices: {', '.join(SUPPORTED_PLATFORMS)}"
            )
        return constructors[platform]()

    # ------------------------------------------------------------------
    # Per-platform builders – each imports the adapter lazily.
    # ------------------------------------------------------------------

    @classmethod
    def _build_cognodb(cls) -> BaseGraphAdapter:
        from .cognodb_adapter import CognoDBAdapter

        return CognoDBAdapter(
            bolt_uri=os.getenv("COGNODB_BOLT_URI", ""),
            username=os.getenv("COGNODB_USERNAME", "neo4j"),
            password=os.getenv("COGNODB_PASSWORD", ""),
            database=os.getenv("COGNODB_DATABASE", "neo4j"),
        )

    @classmethod
    def _build_neo4j(cls) -> BaseGraphAdapter:
        from .neo4j_adapter import Neo4jAdapter

        return Neo4jAdapter(
            bolt_uri=os.getenv("NEO4J_AURA_URI", ""),
            username=os.getenv("NEO4J_AURA_USERNAME", "neo4j"),
            password=os.getenv("NEO4J_AURA_PASSWORD", ""),
            database=os.getenv("NEO4J_AURA_DATABASE", "neo4j"),
        )

    @classmethod
    def _build_memgraph(cls) -> BaseGraphAdapter:
        from .memgraph_adapter import MemgraphAdapter

        return MemgraphAdapter(
            bolt_uri=os.getenv("MEMGRAPH_BOLT_URI", "bolt://localhost:7687"),
            username=os.getenv("MEMGRAPH_USERNAME", ""),
            password=os.getenv("MEMGRAPH_PASSWORD", ""),
            database=os.getenv("MEMGRAPH_DATABASE", "memgraph"),
        )

    @classmethod
    def _build_tigergraph(cls) -> BaseGraphAdapter:
        from .tigergraph_adapter import TigerGraphAdapter

        return TigerGraphAdapter(
            host=os.getenv("TG_HOST", ""),
            username=os.getenv("TG_USERNAME", "tigergraph"),
            password=os.getenv("TG_PASSWORD", ""),
            graphname=os.getenv("TG_GRAPHNAME", "BenchmarkGraph"),
            secret=os.getenv("TG_SECRET", ""),
        )

    @classmethod
    def _build_arcadedb(cls) -> BaseGraphAdapter:
        from .arcadedb_adapter import ArcadeDBAdapter

        return ArcadeDBAdapter(
            host=os.getenv("ARCADEDB_HOST", "http://localhost"),
            http_port=int(os.getenv("ARCADEDB_HTTP_PORT", "2480")),
            graph_name=os.getenv("ARCADEDB_GRAPH_NAME", "BenchmarkGraph"),
            username=os.getenv("ARCADEDB_USERNAME", "root"),
            password=os.getenv("ARCADEDB_PASSWORD", ""),
        )

    @classmethod
    def _build_falkordb(cls) -> BaseGraphAdapter:
        from .falkordb_adapter import FalkorDBAdapter

        return FalkorDBAdapter(
            host=os.getenv("FALKORDB_HOST", "localhost"),
            port=int(os.getenv("FALKORDB_PORT", "6379")),
            graphname=os.getenv("FALKORDB_GRAPHNAME", "BenchmarkGraph"),
        )
