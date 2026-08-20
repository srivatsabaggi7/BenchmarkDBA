"""
Graph Database Benchmark Suite – Main Harness.

Usage
-----
    python -m venv .venv && source .venv/bin/activate
    pip install -r requirements.txt
    cp .env.example .env       # then fill in credentials
    python main.py [--platforms CognoDB Neo4j] [--output results.json]

The harness follows this phase order for every selected platform:
  1. connect + clear_data + create_indices
  2. ingest_batch                          (record ingest throughput)
  3. point_lookup / indexed_lookup /       (cold run → warmup → N warm runs
     traverse 1/2/3-hop / aggregate          with true p50/p95 via numpy)
  4. concurrency sweeps at 1/10/40 clients (80/20 read-write, sustained QPS)
  5. get_footprint
  6. serialise → `benchmark_results.json`
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import os
import platform
import random
import statistics
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable

import numpy as np
from dotenv import load_dotenv

from adapters import AdapterFactory
from core.base_adapter import (
    BaseGraphAdapter,
    BenchmarkMetrics,
    ConcurrencyResult,
    IngestResult,
)

# ---------------------------------------------------------------------------
# Logging & paths
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("benchmark")

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
NODES_CSV = DATA_DIR / "nodes.csv"
RELS_CSV = DATA_DIR / "relationships.csv"
DEFAULT_OUTPUT = ROOT / "benchmark_results.json"


# ---------------------------------------------------------------------------
# CSV loading
# ---------------------------------------------------------------------------


def load_nodes() -> list[dict[str, Any]]:
    """Load `./data/nodes.csv` and coerce numeric fields."""
    if not NODES_CSV.exists():
        raise FileNotFoundError(
            f"Missing {NODES_CSV}.  Place the pre-generated dataset under "
            f"`./data/` before running the harness."
        )
    rows: list[dict[str, Any]] = []
    with NODES_CSV.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            row["reputation_score"] = float(row.get("reputation_score") or 0)
            rows.append(row)
    logger.info("Loaded %d node records from %s", len(rows), NODES_CSV.name)
    return rows


def load_relationships() -> list[dict[str, Any]]:
    """Load `./data/relationships.csv` and coerce numeric fields."""
    if not RELS_CSV.exists():
        raise FileNotFoundError(
            f"Missing {RELS_CSV}.  Place the pre-generated dataset under "
            f"`./data/` before running the harness."
        )
    rows: list[dict[str, Any]] = []
    with RELS_CSV.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            row["weight"] = float(row.get("weight") or 0)
            rows.append(row)
    logger.info("Loaded %d relationship records from %s", len(rows), RELS_CSV.name)
    return rows


# ---------------------------------------------------------------------------
# Tunables (overridable via .env)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Tunables:
    batch_size: int
    warmup_iterations: int
    read_iterations: int
    concurrency_levels: tuple[int, ...]
    read_write_ratio: float
    concurrency_duration_seconds: float
    query_timeout_seconds: float

    @classmethod
    def from_env(cls) -> "Tunables":
        def _int_list(key: str, default: str) -> tuple[int, ...]:
            raw = os.getenv(key, default)
            return tuple(int(v.strip()) for v in raw.split(",") if v.strip())

        return cls(
            batch_size=int(os.getenv("BENCHMARK_BATCH_SIZE", "1000")),
            warmup_iterations=int(os.getenv("BENCHMARK_WARMUP_ITERATIONS", "10")),
            read_iterations=int(os.getenv("BENCHMARK_READ_ITERATIONS", "100")),
            concurrency_levels=_int_list(
                "BENCHMARK_CONCURRENCY_LEVELS", "1,10,40"
            ),
            read_write_ratio=float(
                os.getenv("BENCHMARK_READ_WRITE_RATIO", "0.8")
            ),
            concurrency_duration_seconds=float(
                os.getenv("BENCHMARK_CONCURRENCY_DURATION_SECONDS", "30")
            ),
            query_timeout_seconds=float(
                os.getenv("BENCHMARK_QUERY_TIMEOUT_SECONDS", "30")
            ),
        )


# ---------------------------------------------------------------------------
# Workload helpers
# ---------------------------------------------------------------------------


def _safe_call(
    fn: Callable[[], Any],
    caveats: list[str],
    timeout_seconds: float,
    default: Any = None,
) -> tuple[Any, float, bool]:
    """Call ``fn``, capture wall-time in ms, and cleanly swallow errors.

    Returns ``(result, latency_ms, errored)``.
    """
    t0 = time.perf_counter()
    errored = False
    result = default
    try:
        result = fn()
    except asyncio.TimeoutError as exc:  # pragma: no cover - defensive
        caveats.append(f"Timeout: {exc!r}")
        errored = True
    except Exception as exc:  # pragma: no cover - network errors etc.
        msg = f"{type(exc).__name__}: {exc}"
        logger.warning("Query failed: %s", msg)
        caveats.append(msg)
        errored = True
    dt_ms = (time.perf_counter() - t0) * 1000.0
    if dt_ms / 1000.0 > timeout_seconds and not errored:
        caveats.append(
            f"Slow query ({dt_ms:.1f}ms) exceeded "
            f"{timeout_seconds:.0f}s budget"
        )
    return result, dt_ms, errored


def _warm_percentiles(latencies: list[float]) -> tuple[float | None, float | None]:
    """Compute p50 / p95 with numpy, or None if the list is empty."""
    if not latencies:
        return None, None
    arr = np.asarray(latencies, dtype=np.float64)
    p50 = float(np.percentile(arr, 50))
    p95 = float(np.percentile(arr, 95))
    return p50, p95


def _run_latency_workload(
    label: str,
    adapter: BaseGraphAdapter,
    single_call: Callable[[], Any],
    tunables: Tunables,
) -> BenchmarkMetrics:
    """Cold → warmup → N warm iterations with per-iteration capture.

    ``single_call`` is a zero-arg callable that performs one logical
    workload unit (e.g. one traversal, one point lookup).
    """
    metrics = BenchmarkMetrics()
    caveats = metrics.caveats

    logger.info("  [cold] %s", label)
    _, cold_ms, err = _safe_call(
        single_call, caveats, tunables.query_timeout_seconds
    )
    if err:
        metrics.errors += 1
    else:
        metrics.cold_latency_ms = cold_ms

    logger.info("  [warmup x%d] %s", tunables.warmup_iterations, label)
    for _ in range(tunables.warmup_iterations):
        _safe_call(single_call, caveats, tunables.query_timeout_seconds)

    logger.info("  [warm x%d] %s", tunables.read_iterations, label)
    warm: list[float] = []
    for _ in range(tunables.read_iterations):
        _, dt_ms, err = _safe_call(
            single_call, caveats, tunables.query_timeout_seconds
        )
        if err:
            metrics.errors += 1
            continue
        warm.append(dt_ms)
    metrics.warm_latencies_ms = warm
    metrics.p50_ms, metrics.p95_ms = _warm_percentiles(warm)
    return metrics


# ---------------------------------------------------------------------------
# Concurrency sweep
# ---------------------------------------------------------------------------


async def _concurrency_worker(
    adapter: BaseGraphAdapter,
    stop_event: asyncio.Event,
    node_ids: list[str],
    read_write_ratio: float,
    timeout_seconds: float,
    counter: dict[str, int],
    caveats: list[str],
    seed: int,
) -> None:
    """An individual worker looping until ``stop_event`` is set.

    ``counter`` is mutated in place – keys: ``reads``, ``writes``, ``errors``.
    """
    rng = random.Random(seed)
    min_score = rng.uniform(10, 90)
    hop_choices = (1, 2, 3)

    while not stop_event.is_set():
        is_read = rng.random() < read_write_ratio
        start = rng.choice(node_ids)

        if is_read:
            op_choice = rng.randint(0, 3)
            if op_choice == 0:
                fn = lambda: adapter.point_lookup_async(start)  # noqa: E731
            elif op_choice == 1:
                fn = lambda: adapter.indexed_lookup_async(min_score)  # noqa: E731
            elif op_choice == 2:
                hops = rng.choice(hop_choices)
                fn = lambda h=hops, s=start: adapter.traverse_n_hop_async(s, h)  # noqa: E731
            else:
                fn = adapter.aggregate_async
        else:
            target = rng.choice(node_ids)
            fn = lambda s=start, t=target: adapter.write_query_async(s, t)  # noqa: E731

        try:
            await asyncio.wait_for(
                fn(), timeout=timeout_seconds
            )
        except asyncio.TimeoutError as exc:
            caveats.append(f"Concurrency timeout: {exc!r}")
            counter["errors"] += 1
        except Exception as exc:  # pragma: no cover - defensive
            msg = f"{type(exc).__name__}: {exc}"
            caveats.append(msg)
            counter["errors"] += 1
        else:
            if is_read:
                counter["reads"] += 1
            else:
                counter["writes"] += 1


async def _run_concurrency_level(
    adapter: BaseGraphAdapter,
    clients: int,
    node_ids: list[str],
    tunables: Tunables,
) -> ConcurrencyResult:
    """Spawn ``clients`` workers for ``duration`` seconds and count QPS."""
    caveats: list[str] = []
    counters: list[dict[str, int]] = [
        {"reads": 0, "writes": 0, "errors": 0} for _ in range(clients)
    ]
    stop_event = asyncio.Event()

    workers = [
        _concurrency_worker(
            adapter=adapter,
            stop_event=stop_event,
            node_ids=node_ids,
            read_write_ratio=tunables.read_write_ratio,
            timeout_seconds=tunables.query_timeout_seconds,
            counter=counters[i],
            caveats=caveats,
            seed=i + 1,
        )
        for i in range(clients)
    ]
    logger.info(
        "  [concurrency] clients=%d ratio=%.2f duration=%ss",
        clients,
        tunables.read_write_ratio,
        tunables.concurrency_duration_seconds,
    )
    tasks = [asyncio.create_task(w) for w in workers]
    await asyncio.sleep(tunables.concurrency_duration_seconds)
    stop_event.set()
    await asyncio.gather(*tasks, return_exceptions=True)

    total_reads = sum(c["reads"] for c in counters)
    total_writes = sum(c["writes"] for c in counters)
    total_queries = total_reads + total_writes
    errors = sum(c["errors"] for c in counters)
    dur = tunables.concurrency_duration_seconds
    qps = total_queries / dur if dur > 0 else 0.0
    read_qps = total_reads / dur if dur > 0 else 0.0
    write_qps = total_writes / dur if dur > 0 else 0.0
    return ConcurrencyResult(
        clients=clients,
        read_write_ratio=tunables.read_write_ratio,
        duration_sec=dur,
        total_queries=total_queries,
        total_reads=total_reads,
        total_writes=total_writes,
        qps=qps,
        read_qps=read_qps,
        write_qps=write_qps,
        errors=errors,
        caveats=caveats,
    )


# ---------------------------------------------------------------------------
# Per-platform driver
# ---------------------------------------------------------------------------


def _benchmark_platform(
    name: str,
    nodes: list[dict[str, Any]],
    rels: list[dict[str, Any]],
    tunables: Tunables,
) -> dict[str, Any]:
    """Run every phase against one platform and return the serialisable dict."""
    caveats: list[str] = []
    node_ids = [n["id"] for n in nodes]
    rng = random.Random(42)

    platform_record: dict[str, Any] = {
        "platform": name,
        "version": "Unknown",
        "status": "pending",
        "connect_ok": False,
        "metadata": {
            "os": platform.platform(),
            "python": platform.python_version(),
            "batch_size": tunables.batch_size,
            "warmup_iterations": tunables.warmup_iterations,
            "read_iterations": tunables.read_iterations,
            "read_write_ratio": tunables.read_write_ratio,
            "concurrency_duration_sec": tunables.concurrency_duration_seconds,
        },
        "ingest": None,
        "latencies": {},
        "concurrency": [],
        "footprint": "Not Observable",
        "caveats": caveats,
    }

    adapter: BaseGraphAdapter | None = None
    try:
        logger.info("=" * 60)
        logger.info("Platform: %s", name)
        adapter = AdapterFactory.build(name)
        platform_record["version"] = adapter.platform_version

        # Phase 1 – connect
        logger.info("Phase 1: connect")
        try:
            adapter.connect()
            platform_record["connect_ok"] = True
        except Exception as exc:  # pragma: no cover - network errors
            msg = f"Connect failed: {type(exc).__name__}: {exc}"
            logger.error(msg)
            caveats.append(msg)
            platform_record["status"] = "skipped"
            return platform_record

        # Phase 2 – clear + indices
        logger.info("Phase 2: clear_data + create_indices")
        try:
            adapter.clear_data()
        except Exception as exc:
            caveats.append(f"clear_data failure: {exc!r}")
        try:
            adapter.create_indices()
        except Exception as exc:
            caveats.append(f"create_indices failure: {exc!r}")

        # Phase 3 – ingest
        logger.info("Phase 3: ingest (%d nodes, %d rels)", len(nodes), len(rels))
        ingest: IngestResult | None = None
        try:
            ingest = adapter.ingest_batch(
                nodes=nodes,
                relationships=rels,
                batch_size=tunables.batch_size,
            )
            caveats.extend(ingest.caveats)
            platform_record["ingest"] = ingest.to_dict()
        except Exception as exc:
            msg = f"Ingestion failed: {type(exc).__name__}: {exc}"
            logger.error(msg)
            caveats.append(msg)
            platform_record["status"] = "partial"

        # Phase 4 – latency workloads (requires populated graph)
        lat_record: dict[str, Any] = {}
        if ingest is not None and ingest.nodes_ingested > 0:
            logger.info("Phase 4: latency workloads")
            sample_lookup_id = node_ids[len(node_ids) // 2]
            sample_min_score = 50.0

            workloads = [
                (
                    "point_lookup",
                    lambda s=sample_lookup_id: adapter.point_lookup(s),
                ),
                (
                    "indexed_lookup",
                    lambda sc=sample_min_score: adapter.indexed_lookup(sc),
                ),
                (
                    "traverse_1_hop",
                    lambda s=sample_lookup_id: adapter.traverse_n_hop(s, 1),
                ),
                (
                    "traverse_2_hop",
                    lambda s=sample_lookup_id: adapter.traverse_n_hop(s, 2),
                ),
                (
                    "traverse_3_hop",
                    lambda s=sample_lookup_id: adapter.traverse_n_hop(s, 3),
                ),
                ("aggregate", adapter.aggregate),
            ]
            for label, fn in workloads:
                try:
                    m = _run_latency_workload(label, adapter, fn, tunables)
                    lat_record[label] = m.to_dict()
                    caveats.extend(m.caveats)
                except Exception as exc:  # pragma: no cover
                    msg = f"Workload {label} failed: {exc!r}"
                    logger.error(msg)
                    caveats.append(msg)
                    lat_record[label] = BenchmarkMetrics(
                        caveats=[msg], errors=1
                    ).to_dict()
        platform_record["latencies"] = lat_record

        # Phase 5 – concurrency sweeps
        concurrency_record: list[dict[str, Any]] = []
        if ingest is not None and ingest.nodes_ingested > 0:
            logger.info("Phase 5: concurrency sweeps %s", tunables.concurrency_levels)
            try:
                for clients in tunables.concurrency_levels:
                    res = asyncio.run(
                        _run_concurrency_level(
                            adapter, clients, node_ids, tunables
                        )
                    )
                    concurrency_record.append(res.to_dict())
                    caveats.extend(res.caveats)
            except Exception as exc:  # pragma: no cover
                msg = f"Concurrency sweep aborted: {exc!r}"
                logger.error(msg)
                caveats.append(msg)
        platform_record["concurrency"] = concurrency_record

        # Phase 6 – footprint
        try:
            platform_record["footprint"] = adapter.get_footprint()
        except Exception as exc:
            caveats.append(f"get_footprint failure: {exc!r}")

        platform_record["status"] = (
            "ok"
            if not any("failed" in c.lower() for c in caveats)
            else "partial"
        )
    finally:
        if adapter is not None:
            try:
                adapter.disconnect()
            except Exception:
                pass
    return platform_record


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Graph Database Benchmark Suite"
    )
    parser.add_argument(
        "--platforms",
        nargs="+",
        choices=AdapterFactory.available_platforms(),
        default=AdapterFactory.available_platforms(),
        help="Which platforms to run (default: all five).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Destination JSON file (default: ./benchmark_results.json).",
    )
    return parser.parse_args(argv)


def build_dev_log(
    platforms: list[dict[str, Any]], dataset_info: dict[str, Any]
) -> list[dict[str, str]]:
    """Build the decisions panel content from the outcome of this run."""
    entries: list[dict[str, str]] = []
    for record in platforms:
        name = str(record.get("name") or record.get("platform") or "Database")
        status = str(record.get("status") or "").lower()
        note = str(record.get("status_note") or "").strip()
        caveats = "; ".join(str(item) for item in record.get("caveats", []))
        body = " ".join(part for part in (note, caveats) if part).strip()

        if status in {"skipped", "dnf"}:
            entries.append({
                "title": f"Why {name} is marked DNF",
                "body": body or "The platform did not complete this benchmark run.",
            })
        elif status == "partial":
            entries.append({
                "title": f"{name} — partial result",
                "body": body or "The platform completed only part of this benchmark run.",
            })

    succeeded = [
        str(record.get("name") or record.get("platform"))
        for record in platforms
        if str(record.get("status", "")).lower() in {"ok", "success"}
    ]
    entries.append({
        "title": "Comparators used in this run",
        "body": (
            f"Fully successful: {', '.join(succeeded) or 'none'}. "
            f"Dataset: {dataset_info.get('nodes', 0):,} nodes, "
            f"{dataset_info.get('relationships', 0):,} relationships from "
            f"{dataset_info.get('source', 'the local benchmark dataset')}."
        ),
    })
    return entries


def build_raw_run_log(
    platforms: list[dict[str, Any]], timestamp: str
) -> list[dict[str, str]]:
    """Serialise recorded run outcomes into the dashboard's log-table shape."""
    rows: list[dict[str, str]] = []
    for record in platforms:
        database = str(record.get("name") or record.get("platform") or "Database")
        rows.append({
            "timestamp": timestamp,
            "db": database,
            "event": "Connection",
            "status": "success" if record.get("connect_ok") else "failed",
        })
        if record.get("ingest"):
            rows.append({
                "timestamp": timestamp,
                "db": database,
                "event": "Data ingestion",
                "status": str(record.get("status", "unknown")),
            })
        for sweep in record.get("concurrency", []):
            rows.append({
                "timestamp": timestamp,
                "db": database,
                "event": f"Concurrency sweep: {sweep.get('clients', 0)} clients",
                "status": "success" if not sweep.get("errors") else f"{sweep['errors']} errors",
            })
    return rows


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    load_dotenv(ROOT / ".env")
    tunables = Tunables.from_env()

    logger.info("Tunables: %s", asdict(tunables))
    nodes = load_nodes()
    rels = load_relationships()

    results: dict[str, Any] = {
        "suite_version": "1.0.0",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "dataset": {
            "nodes": len(nodes),
            "relationships": len(rels),
            "source": "data/nodes.csv + data/relationships.csv",
            "nodes_csv": str(NODES_CSV),
            "relationships_csv": str(RELS_CSV),
        },
        "tunables": asdict(tunables),
        "platforms": [],
    }

    for name in args.platforms:
        rec = _benchmark_platform(name, nodes, rels, tunables)
        results["platforms"].append(rec)

    results["dev_log"] = build_dev_log(results["platforms"], results["dataset"])
    results["raw_run_log"] = build_raw_run_log(
        results["platforms"], results["generated_at"]
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as fh:
        json.dump(results, fh, indent=2, default=str)
    logger.info("Wrote results → %s", args.output.resolve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
