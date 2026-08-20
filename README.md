# CognoDB Cloud vs. Managed Graph Databases — A Fairness-First Benchmark
Benchmarking CognoDB Cloud against five other graph database platforms under matched resource limits.

## Why this benchmark, and how to read it
Every graph database in this comparison received the same dataset, the same logical queries, the same client, and the same resource ceiling — CognoDB's own free-tier limits (0.5 vCPU / 256 MB RAM / 1 GB disk). The goal isn't to declare a winner. It's to answer a narrower, more useful question: under identical, honestly-documented constraints, how do these platforms actually behave?

Where a platform failed to run, that's reported as plainly as a platform that succeeded — a clean sweep isn't the goal here, a defensible one is.

## 1. Databases compared
| Platform | Role | Deployment | Status |
| :--- | :--- | :--- | :--- |
| **CognoDB Cloud** | Benchmark target | Managed free tier (c0) | ⚠️ Partial — see caveats |
| **Neo4j AuraDB Free** | Comparator | Managed free tier | ✅ Success |
| **Memgraph (Community Edition)** | Comparator | Self-hosted, Docker, capped to CognoDB's specs | ✅ Success[cite: 1] |
| **FalkorDB** | Comparator | Self-hosted, Docker, capped to CognoDB's specs | ✅ Success[cite: 1] |
| **TigerGraph Cloud** | Comparator | Managed free tier | ❌ DNF — see caveats[cite: 1] |
| **ArcadeDB** | Comparator | Self-hosted, Docker, capped to CognoDB's specs | ❌ DNF — see caveats[cite: 1] |

Why these six. Neo4j AuraDB Free was chosen first because CognoDB itself connects via the official Neo4j Bolt driver — comparing against Aura isolates "managed service quality" from query-language differences, giving a clean apples-to-apples baseline. Memgraph and FalkorDB were self-hosted via Docker and capped explicitly to CognoDB's own resource numbers (`--cpus=0.5 --memory=256m`), which sidesteps the ambiguity of matching an external vendor's free-tier specs and gives literal, verifiable parity instead. TigerGraph Cloud was included for genuine architectural diversity — a distributed, graph-native engine rather than a Bolt/Cypher-based store. ArcadeDB was attempted as a second self-hosted, multi-model comparator.

## 2. Resource parity (fairness)
| Platform | vCPU | RAM | Disk | Tier / method |
| :--- | :--- | :--- | :--- | :--- |
| **CognoDB Cloud** | 0.5 (burstable) | 256 MB | 1 GB | Free tier (c0) |
| **Neo4j AuraDB Free** | Not Published | Not Published | 1 GB | Free tier |
| **Memgraph** | 0.5 | 256 MB | 1 GB | Self-hosted, Docker-capped |
| **FalkorDB** | 0.5 | 256 MB | 1 GB | Self-hosted, Docker-capped |
| **TigerGraph Cloud** | N/A | N/A | N/A | Free tier — connection never succeeded[cite: 1] |
| **ArcadeDB** | 0.5 (attempted) | 256 MB (attempted) | 1 GB | Self-hosted, Docker-capped — see DNF note[cite: 1] |

Where a managed platform's free tier doesn't expose an exact vCPU/RAM number, that's stated here rather than guessed at — the goal is equivalent limits, not fabricated symmetry.

## 3. Dataset
*   **Source:** `data/nodes.csv` and `data/relationships.csv`[cite: 1]
*   **Nodes:** 47,000[cite: 1]
*   **Relationships:** 160,000[cite: 1]
*   **Sampling method:** Static flat-file loading in batches of 1,000[cite: 1]
*   **Loaded identically into every platform, via:** Parameterized Python driver executing batch Cypher transactions[cite: 1]. 

## 4. Methodology
*   **Warm-up:** Every platform was warmed up with 10 iterations before any measured run[cite: 1]; cold-start numbers are reported separately, not mixed in.
*   **Read workloads:** 100 measured iterations per workload after warm-up[cite: 1], from a randomly selected set of start nodes, with p50 and p95 reported rather than averages alone. 
*   **Mixed workload:** Concurrent read/write test at client concurrency levels of 1, 10, and 40[cite: 1], running for a 30.0-second duration[cite: 1] with a fixed read/write ratio of 0.8 (80/20)[cite: 1] held identical across platforms.
*   **Same logical queries everywhere:** Where query syntax necessarily differed between platforms, the same logical operation was implemented rather than forcing identical syntax.
*   **Client:** All benchmarks were run from a local Windows 11 machine (OS Build 10.0.26200-SP0) running Python 3.14.6[cite: 1].
*   **Automation:** The full pipeline — load, benchmark, results generation, dashboard build — runs from a single command (see Reproducibility below).

## 5. Results

### 5.1 Data loading
| Platform | Nodes/sec | Relationships/sec | Total load time |
| :--- | :--- | :--- | :--- |
| **CognoDB Cloud** | 211.29[cite: 1] | 719.29[cite: 1] | 222.44s[cite: 1] |
| **Neo4j AuraDB Free** | 663.68[cite: 1] | 2259.33[cite: 1] | 70.82s[cite: 1] |
| **Memgraph** | 1926.29[cite: 1] | 6557.59[cite: 1] | 24.40s[cite: 1] |
| **FalkorDB** | 6448.98[cite: 1] | 21953.98[cite: 1] | 7.29s[cite: 1] |
| **TigerGraph Cloud** | N/A (DNF)[cite: 1] | N/A (DNF)[cite: 1] | N/A (DNF)[cite: 1] |
| **ArcadeDB** | N/A (DNF)[cite: 1] | N/A (DNF)[cite: 1] | N/A (DNF)[cite: 1] |

### 5.2 Traversals (p50 / p95, ms)
| Platform | 1-hop | 2-hop | 3-hop |
| :--- | :--- | :--- | :--- |
| **CognoDB Cloud** | 317.99 / 465.93[cite: 1] | 325.90 / 468.68[cite: 1] | 348.03 / 453.46[cite: 1] |
| **Neo4j AuraDB Free** | 100.29 / 170.57[cite: 1] | 101.47 / 172.00[cite: 1] | 93.00 / 125.21[cite: 1] |
| **Memgraph** | 0.91 / 2.37[cite: 1] | 1.65 / 2.94[cite: 1] | 12.75 / 17.60[cite: 1] |
| **FalkorDB** | 0.75 / 3.03[cite: 1] | 2.79 / 4.92[cite: 1] | 17.88 / 25.47[cite: 1] |
| **TigerGraph Cloud** | N/A[cite: 1] | N/A[cite: 1] | N/A[cite: 1] |
| **ArcadeDB** | N/A[cite: 1] | N/A[cite: 1] | N/A[cite: 1] |

### 5.3 Lookups (p50 / p95, ms)
| Platform | Point lookup | Indexed lookup |
| :--- | :--- | :--- |
| **CognoDB Cloud** | 323.38 / 449.60[cite: 1] | 1335.21 / 1706.56[cite: 1] |
| **Neo4j AuraDB Free** | 104.47 / 158.17[cite: 1] | 119.95 / 130.62[cite: 1] |
| **Memgraph** | 0.99 / 5.04[cite: 1] | 121.05 / 193.28[cite: 1] |
| **FalkorDB** | 0.79 / 4.68[cite: 1] | 27.00 / 35.35[cite: 1] |
| **TigerGraph Cloud** | N/A[cite: 1] | N/A[cite: 1] |
| **ArcadeDB** | N/A[cite: 1] | N/A[cite: 1] |

### 5.4 Aggregation (p50 / p95, ms)
| Platform | p50 | p95 |
| :--- | :--- | :--- |
| **CognoDB Cloud** | 417.40[cite: 1] | 504.23[cite: 1] |
| **Neo4j AuraDB Free** | 111.48[cite: 1] | 155.81[cite: 1] |
| **Memgraph** | 14.79[cite: 1] | 69.75[cite: 1] |
| **FalkorDB** | 9.35[cite: 1] | 56.26[cite: 1] |
| **TigerGraph Cloud** | N/A[cite: 1] | N/A[cite: 1] |
| **ArcadeDB** | N/A[cite: 1] | N/A[cite: 1] |

### 5.5 Mixed read/write workload (QPS by concurrency)
| Platform | 1 client | 10 clients | 40 clients | Errors/timeouts |
| :--- | :--- | :--- | :--- | :--- |
| **CognoDB Cloud** | 1.60 QPS[cite: 1] | 4.47 QPS[cite: 1] | 3.73 QPS[cite: 1] | 4 TimeoutErrors at 40 clients[cite: 1] |
| **Neo4j AuraDB Free** | 6.83 QPS[cite: 1] | 65.13 QPS[cite: 1] | 72.60 QPS[cite: 1] | None[cite: 1] |
| **Memgraph** | 7.67 QPS[cite: 1] | 13.30 QPS[cite: 1] | 7.77 QPS[cite: 1] | 1 TimeoutError at 10 clients; 1 TimeoutError at 40 clients[cite: 1] |
| **FalkorDB** | 52.33 QPS[cite: 1] | 73.87 QPS[cite: 1] | 71.43 QPS[cite: 1] | None[cite: 1] |
| **TigerGraph Cloud** | N/A[cite: 1] | N/A[cite: 1] | N/A[cite: 1] | TG_HOST not configured[cite: 1] |
| **ArcadeDB** | N/A[cite: 1] | N/A[cite: 1] | N/A[cite: 1] | HTTP 409 Conflict during ingestion[cite: 1] |

### 5.6 Footprint
| Platform | Storage / memory | Method |
| :--- | :--- | :--- |
| **CognoDB Cloud** | Count-based proxy: 47,000 nodes, 160,049 rels | Extracted via Cypher count because `db.stats.retrieve` was unavailable[cite: 1] |
| **Neo4j AuraDB Free** | Not Observable (approx 30,990 KB) | Console reading[cite: 1] |
| **Memgraph** | 245.21 MiB resident, 147.05 MiB disk usage | Native internal memory tracker[cite: 1] |
| **FalkorDB** | Not Observable | Client method missing[cite: 1] |
| **TigerGraph Cloud** | Not Observable[cite: 1] | N/A — never connected[cite: 1] |
| **ArcadeDB** | Not Observable[cite: 1] | N/A — never started[cite: 1] |

## 6. Caveats & Development Decisions
*   **CognoDB Cloud** experienced intermittent connection drops during the higher-concurrency portion of the mixed workload, manifesting as `TimeoutError()` at the 40-client tier[cite: 1]. Retry-with-backoff and connection pool tuning were applied; the drop rate itself is reported as a finding, not smoothed over.
*   **Memgraph** recorded minor instability under concurrent load, yielding one `TimeoutError()` at the 10-client tier and one `TimeoutError()` at the 40-client tier[cite: 1].
*   **The ArcadeDB Problem (DNF):** ArcadeDB is marked DNF[cite: 1]. Under the strict 256 MB RAM cap required to ensure fairness, its Java Virtual Machine (JVM) overhead was unable to handle the batched ingestion phase. When processing concurrent write transactions, it produced an `HTTP 409 Conflict` error during ingestion[cite: 1]. Because allocating extra memory to resolve this would violate the foundational resource constraints of the benchmark, the database was dropped and the failure documented.
*   **The TigerGraph Cloud Pivot (DNF):** TigerGraph Cloud is marked DNF[cite: 1]. I encountered significant challenges connecting programmatically due to a recent change in their connection and authentication model introduced in the Savanna upgrade[cite: 1]. The new workspace routing and API token generation mechanisms blocked headless automation and prevented us from establishing a successful connection within the allocated time limit, throwing a `TG_HOST not configured` connection error[cite: 1]. Consequently, I executed a strategic pivot to use FalkorDB locally, capping its Docker container at identical specs to maintain the 5-database requirement.
*   A sixth platform (Kuzu) was considered as a replacement for TigerGraph but not pursued — it requires Python 3.12, while the rest of the benchmark environment runs Python 3.14.6[cite: 1], and downgrading risked breaking the platforms that were already working correctly this close to the deadline.

## 7. Analysis
1.  **In-Memory Efficiency:** The strictly in-memory engines capped at 256MB RAM (Memgraph and FalkorDB) significantly outperformed the disk-backed systems (Neo4j and CognoDB) on traversal speeds. FalkorDB completed 1-hop traversals at a p50 of 0.75ms[cite: 1], and Memgraph completed them at 0.91ms[cite: 1]. By comparison, CognoDB required 317.99ms[cite: 1] and Neo4j required 100.29ms[cite: 1].
2.  **Concurrency Bottlenecks:** CognoDB's burstable tier demonstrated a firm concurrency ceiling. During the 40-client mixed workload sweep, it plateaued at 3.73 QPS and dropped connections, producing four distinct timeout errors[cite: 1]. Neo4j and FalkorDB both scaled cleanly to over 71 QPS at the 40-client mark with zero errors[cite: 1].
3.  **Throughput vs. Latency:** While FalkorDB boasted the fastest raw ingestion speed (21,953.98 relationships per second)[cite: 1] and the highest single-client QPS (52.33)[cite: 1], Neo4j closed the gap under heavy concurrent stress, successfully managing 72.60 QPS at the 40-client mark[cite: 1].

## 8. Reproducibility
```bash
git clone <your-repo-url>
cd <repo-name>
cp .env.example .env

# fill in your own free-tier credentials for CognoDB, AuraDB, TigerGraph (if retrying) — never commit real values

python -m venv .venv
.\.venv\Scripts\pip.exe install -r requirements.txt
.\.venv\Scripts\python.exe main.py
```
These commands load the dataset into every platform, run the full workload suite, write `benchmark_results.json`, and prepare the results for the dashboard.

> **Security:** No credentials or connection URIs are committed anywhere in this repository. All secrets are read from environment variables at runtime.

## 9. Dashboard

A visual results dashboard is included in `dashboard/`. It renders `benchmark_results.json`, including a full run log.

### Run the React + Vite Dashboard

```bash
cd dashboard
npm install
npx vite --host 127.0.0.1 --port 5173
```
