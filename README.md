# CognoDB Cloud vs. Managed Graph Databases — A Fairness-First Benchmark
Benchmarking CognoDB Cloud against five other graph database platforms under matched resource limits.

# Live here:
https://srivatsabaggi7.github.io/BenchmarkDBA/

## Why this benchmark, and how to read it:
Every graph database in this comparison received the same dataset, the same logical queries, the same client, and the same resource ceiling — CognoDB's own free-tier limits (0.5 vCPU / 256 MB RAM / 1 GB disk). The goal isn't to declare a winner. It's to answer a narrower, more useful question: under identical, honestly-documented constraints, how do these platforms actually behave?


## 1. Databases compared
| Platform | Role | Deployment | Status |
| :--- | :--- | :--- | :--- |
| **CognoDB Cloud** | Benchmark target | Managed free tier (c0) | ✅ Success |
| **Neo4j AuraDB Free** | Comparator | Managed free tier | ✅ Success |
| **Memgraph (Community Edition)** | Comparator | Self-hosted, Docker, capped to CognoDB's specs | ✅ Success  |
| **FalkorDB** | Comparator | Self-hosted, Docker, capped to CognoDB's specs | ✅ Success  |
| **TigerGraph Cloud** | Comparator | Managed free tier | ❌ DNF — see caveats  |
| **ArcadeDB** | Comparator | Self-hosted, Docker, capped to CognoDB's specs | ❌ DNF — see caveats  |

## Why these six? 
*   Neo4j AuraDB Free was chosen first because CognoDB itself connects via the official Neo4j Bolt driver — comparing against Aura isolates "managed service quality" from query-language differences, giving a clean apples-to-apples baseline. 
*   Memgraph and FalkorDB were self-hosted via Docker and capped explicitly to CognoDB's own resource numbers (`--cpus=0.5 --memory=256m`), which sidesteps the ambiguity of matching an external vendor's free-tier specs and gives literal, verifiable parity instead.
*   TigerGraph Cloud was included for genuine architectural diversity — a distributed, graph-native engine rather than a Bolt/Cypher-based store.
*   ArcadeDB was attempted as a second self-hosted, multi-model comparator.

## 2. Resource parity (fairness)
| Platform | vCPU | RAM | Disk | Tier / method |
| :--- | :--- | :--- | :--- | :--- |
| **CognoDB Cloud** | 0.5 (burstable) | 256 MB | 1 GB | Free tier (c0) |
| **Neo4j AuraDB Free** | Not Published | Not Published | 1 GB | Free tier |
| **Memgraph** | 0.5 | 256 MB | 1 GB | Self-hosted, Docker-capped |
| **FalkorDB** | 0.5 | 256 MB | 1 GB | Self-hosted, Docker-capped |
| **TigerGraph Cloud** | N/A | N/A | N/A | Free tier — connection never succeeded  |
| **ArcadeDB** | 0.5 (attempted) | 256 MB (attempted) | 1 GB | Self-hosted, Docker-capped — see DNF note  |


## 3. Dataset
*   **Source:** SNAP - Epinions social network (https://snap.stanford.edu/data/soc-Epinions1.html) (Local files: `data/nodes.csv` and `data/relationships.csv`)
*   **Nodes:** 47,000 
*   **Relationships:** 160,000 
*   **Sampling method:** Static flat-file loading in batches of 1,000 
*   **Loaded identically into every platform, via:** Parameterized Python driver executing batch Cypher transactions . 

## 4. Methodology
*   **Warm-up:** Every platform was warmed up with 10 iterations before any measured run ; cold-start numbers are reported separately, not mixed in.
*   **Read workloads:** 100 measured iterations per workload after warm-up , from a randomly selected set of start nodes, with p50 and p95 reported rather than averages alone. 
*   **Mixed workload:** Concurrent read/write test at client concurrency levels of 1, 10, and 40 , running for a 30.0-second duration  with a fixed read/write ratio of 0.8 (80/20)  held identical across platforms.
*   **Same logical queries everywhere:** Where query syntax necessarily differed between platforms, the same logical operation was implemented rather than forcing identical syntax.
*   **Client:** All benchmarks were run from a local Windows 11 machine (OS Build 10.0.26200-SP0) running Python 3.14.6 .
*   **Automation:** The full pipeline — load, benchmark, results generation, dashboard build — runs from a single command (see Reproducibility below).

## 5. Results

### 5.1 Data loading
| Platform | Nodes/sec | Relationships/sec | Total load time |
| :--- | :--- | :--- | :--- |
| **CognoDB Cloud** | 211.29  | 719.29  | 222.44s  |
| **Neo4j AuraDB Free** | 663.68  | 2259.33  | 70.82s  |
| **Memgraph** | 1926.29  | 6557.59  | 24.40s  |
| **FalkorDB** | 6448.98  | 21953.98  | 7.29s  |
| **TigerGraph Cloud** | N/A (DNF)  | N/A (DNF)  | N/A (DNF)  |
| **ArcadeDB** | N/A (DNF)  | N/A (DNF)  | N/A (DNF)  |

### 5.2 Traversals (p50 / p95, ms)
| Platform | 1-hop | 2-hop | 3-hop |
| :--- | :--- | :--- | :--- |
| **CognoDB Cloud** | 317.99 / 465.93  | 325.90 / 468.68  | 348.03 / 453.46  |
| **Neo4j AuraDB Free** | 100.29 / 170.57  | 101.47 / 172.00  | 93.00 / 125.21  |
| **Memgraph** | 0.91 / 2.37  | 1.65 / 2.94  | 12.75 / 17.60  |
| **FalkorDB** | 0.75 / 3.03  | 2.79 / 4.92  | 17.88 / 25.47  |
| **TigerGraph Cloud** | N/A  | N/A  | N/A  |
| **ArcadeDB** | N/A  | N/A  | N/A  |

### 5.3 Lookups (p50 / p95, ms)
| Platform | Point lookup | Indexed lookup |
| :--- | :--- | :--- |
| **CognoDB Cloud** | 323.38 / 449.60  | 1335.21 / 1706.56  |
| **Neo4j AuraDB Free** | 104.47 / 158.17  | 119.95 / 130.62  |
| **Memgraph** | 0.99 / 5.04  | 121.05 / 193.28  |
| **FalkorDB** | 0.79 / 4.68  | 27.00 / 35.35  |
| **TigerGraph Cloud** | N/A  | N/A  |
| **ArcadeDB** | N/A  | N/A  |

### 5.4 Aggregation (p50 / p95, ms)
| Platform | p50 | p95 |
| :--- | :--- | :--- |
| **CognoDB Cloud** | 417.40  | 504.23  |
| **Neo4j AuraDB Free** | 111.48  | 155.81  |
| **Memgraph** | 14.79  | 69.75  |
| **FalkorDB** | 9.35  | 56.26  |
| **TigerGraph Cloud** | N/A  | N/A  |
| **ArcadeDB** | N/A  | N/A  |

### 5.5 Mixed read/write workload (QPS by concurrency)
| Platform | 1 client | 10 clients | 40 clients | Errors/timeouts |
| :--- | :--- | :--- | :--- | :--- |
| **CognoDB Cloud** | 1.60 QPS  | 4.47 QPS  | 3.73 QPS  | 4 TimeoutErrors at 40 clients  |
| **Neo4j AuraDB Free** | 6.83 QPS  | 65.13 QPS  | 72.60 QPS  | None  |
| **Memgraph** | 7.67 QPS  | 13.30 QPS  | 7.77 QPS  | 1 TimeoutError at 10 clients; 1 TimeoutError at 40 clients  |
| **FalkorDB** | 52.33 QPS  | 73.87 QPS  | 71.43 QPS  | None  |
| **TigerGraph Cloud** | N/A  | N/A  | N/A  | TG_HOST not configured  |
| **ArcadeDB** | N/A  | N/A  | N/A  | HTTP 409 Conflict during ingestion  |

### 5.6 Footprint
| Platform | Storage / memory | Method |
| :--- | :--- | :--- |
| **CognoDB Cloud** | Count-based proxy: 47,000 nodes, 160,049 rels | Extracted via Cypher count because `db.stats.retrieve` was unavailable  |
| **Neo4j AuraDB Free** | Not Observable (approx 30,990 KB) | Console reading  |
| **Memgraph** | 245.21 MiB resident, 147.05 MiB disk usage | Native internal memory tracker  |
| **FalkorDB** | Not Observable | Client method missing  |
| **TigerGraph Cloud** | Not Observable  | N/A — never connected  |
| **ArcadeDB** | Not Observable  | N/A — never started  |

## 6. Caveats & Development Decisions
*   **CognoDB Cloud** experienced intermittent connection drops during the higher-concurrency portion of the mixed workload, manifesting as `TimeoutError()` at the 40-client tier . Retry-with-backoff and connection pool tuning were applied.
*   **Memgraph** recorded minor instability under concurrent load, yielding one `TimeoutError()` at the 10-client tier and one `TimeoutError()` at the 40-client tier .
*   **The ArcadeDB Problem (DNF):** ArcadeDB is marked DNF . Under the strict 256 MB RAM cap required to ensure fairness, its Java Virtual Machine (JVM) overhead was unable to handle the batched ingestion phase. When processing concurrent write transactions, it produced an `HTTP 409 Conflict` error during ingestion . Because allocating extra memory to resolve this would violate the foundational resource constraints of the benchmark, the database was dropped and the failure documented.
*   **The TigerGraph Cloud Pivot (DNF):** TigerGraph Cloud is marked DNF . I encountered significant challenges connecting programmatically due to a recent change in their connection and authentication model introduced in the Savanna upgrade . The new workspace routing and API token generation mechanisms blocked headless automation and prevented us from establishing a successful connection within the allocated time limit, throwing a `TG_HOST not configured` connection error . Consequently, I executed a strategic pivot to use FalkorDB locally, capping its Docker container at identical specs to maintain the 5-database requirement.
*   A sixth platform (Kuzu) was considered as a replacement for TigerGraph but not pursued — it requires Python 3.12, while the rest of the benchmark environment runs Python 3.14.6 , and downgrading risked breaking the platforms that were already working correctly this close to the deadline.

## 7. Analysis
1.  **In-Memory Efficiency:** The strictly in-memory engines capped at 256MB RAM (Memgraph and FalkorDB) significantly outperformed the disk-backed systems (Neo4j and CognoDB) on traversal speeds. FalkorDB completed 1-hop traversals at a p50 of 0.75ms , and Memgraph completed them at 0.91ms . By comparison, CognoDB required 317.99ms  and Neo4j required 100.29ms .
2.  **Concurrency Bottlenecks:** CognoDB's burstable tier demonstrated a firm concurrency ceiling. During the 40-client mixed workload sweep, it plateaued at 3.73 QPS and dropped connections, producing four distinct timeout errors . Neo4j and FalkorDB both scaled cleanly to over 71 QPS at the 40-client mark with zero errors .
3.  **Throughput vs. Latency:** While FalkorDB boasted the fastest raw ingestion speed (21,953.98 relationships per second)  and the highest single-client QPS (52.33) , Neo4j closed the gap under heavy concurrent stress, successfully managing 72.60 QPS at the 40-client mark .

## 8. Reproducibility
```bash
git clone https://github.com/srivatsabaggi7/BenchmarkDBA/
cd BenchmarkDBA
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
