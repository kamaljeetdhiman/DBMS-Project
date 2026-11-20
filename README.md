# IoT Time-Series Database Research Project: TimescaleDB vs. PostgreSQL Baseline

An empirical evaluation comparing specialized Time-Series Database (TSDB) architectures (**TimescaleDB**) against standard Relational Database Management Systems (**PostgreSQL 16**) for high-frequency Internet of Things (IoT) sensor logging.

**Group G13 (Department of Information Technology, Dr. B. R. Ambedkar National Institute of Technology Jalandhar):**
- Khushpreet Kaur (Roll No. 25124051)
- Krish Sharma (Roll No. 25124052)
- Kamaljeet Singh (Roll No. 25124050)
- Jitendra Chaudhary (Roll No. 25124049)

---

## Deliverables & Artifacts
- **Interactive Evaluation Dashboard:** [`index.html`](index.html) *(Double-click `start_interface.bat` or open `index.html` in your browser)*
- **Dashboard Launcher Scripts:** [`start_interface.bat`](start_interface.bat) (Windows), [`start_interface.sh`](start_interface.sh) (Linux/macOS), [`app.py`](app.py) (Python local server)
- **Research Paper (Compiled PDF):** [`DBMS_Project_2.pdf`](DBMS_Project_2.pdf)
- **Research Paper (IEEE LaTeX Source):** [`paper.tex`](paper.tex)
- **Plagiarism & Authenticity Report:** [`plagiarism_report.pdf`](plagiarism_report.pdf)
- **Presentation Deck (PPTX):** [`G13_TimescaleDB_Presentation.pptx`](G13_TimescaleDB_Presentation.pptx)
- **Empirical Results (JSON / CSV):** [`benchmark_output/benchmark_results.json`](benchmark_output/benchmark_results.json), [`benchmark_output/benchmark_summary.csv`](benchmark_output/benchmark_summary.csv)

---

## ⚡ Quick Evaluation: Interactive Web Dashboard

To help non-technical evaluators, professors, and reviewers explore the project findings without running command-line benchmarks or inspecting database schemas:

1. **Option 1 (Double-Click Launcher - Windows):**
   - Double-click `start_interface.bat`. It starts a local server and opens your web browser automatically.
2. **Option 2 (Python Launcher):**
   - Run `python app.py` in your terminal. Navigate to [http://localhost:8080](http://localhost:8080).
3. **Option 3 (Direct Browser Open):**
   - Double-click or open `index.html` directly in Google Chrome, Microsoft Edge, or Mozilla Firefox.

### Dashboard Highlights:
- **Query Execution Profiler:** Multi-stage animated execution pipeline showing query acceleration (12 ms vs 518.9 ms) across 1,209,600 tuples with live buffer page tracking.
- **Physical Storage Analysis:** Component disk breakdown comparing raw heap and B-tree index bloat against Gorilla-compressed columnar chunks (83.53% disk reduction).
- **20-Node Telemetry Fleet:** Interactive factory floor matrix across 4 industrial zones showing real-time temperature, humidity, and battery decay curves.
- **System Internals & Execution Plans:** Technical breakdown of hypertable chunk routing, Gorilla bit-packing mechanics, and side-by-side `EXPLAIN ANALYZE` tree diffs.
- **Academic Deliverables:** One-click links for the IEEE research paper, presentation deck, verified plagiarism audit, and source code.


---

## 1. System Architecture & Workload Profile

```
+-----------------------------------------------------------------------------------------+
|                                SYNTHETIC IOT GENERATOR                                  |
|   20 IoT Sensor Nodes  |  Sampling: 10s  |  Duration: 7 Days  |  Volume: 1,209,600 Rows |
|   Realistic Diurnal Sinusoids + Gaussian Micro-Noise + Humidity & Battery Decay Curves  |
+-----------------------------------------------------------------------------------------+
                                     |
              +----------------------+----------------------+
              |                                             |
              v                                             v
+-------------------------------+             +-------------------------------+
|  TimescaleDB 16 (Hypertable)  |             |   Plain PostgreSQL 16 (Heap)  |
| - Daily Temporal Partitioning |             | - Monolithic Heap Table       |
| - Gorilla / Delta-of-Delta    |             | - Traditional B-Tree on time  |
| - Continuous Aggregate (cagg) |             | - Composite (sensor_id, time) |
+-------------------------------+             +-------------------------------+
              |                                             |
              +----------------------+----------------------+
                                     |
                                     v
+-----------------------------------------------------------------------------------------+
|                                    GRAFANA DASHBOARD                                    |
|   Port 3000 | Automated Provisioning | Continuous Aggregates vs. Raw Group-By Queries   |
+-----------------------------------------------------------------------------------------+
```

### Telemetry Formulation
Each sensor $i \in \{1, \dots, 20\}$ simulates environmental temperature, relative humidity, and battery degradation over $t \in [0, 604800]$ seconds:

$$\text{Temp}_i(t) = T_{\text{base}, i} + A_i \sin\left(\frac{2\pi (t - 21600)}{86400} + \phi_i\right) + \epsilon(t), \quad \epsilon(t) \sim \mathcal{N}(0, 0.25^2)$$

$$\text{Hum}_i(t) = H_{\text{base}, i} - 0.8 \cdot (\text{Temp}_i(t) - T_{\text{base}, i}) + \eta(t), \quad \eta(t) \sim \mathcal{N}(0, 0.4^2)$$

$$\text{Bat}_i(t) = 100.0 - \left(\frac{t}{604800}\right) \cdot \Delta B_i + \text{noise}$$

---

## 2. Directory Structure

```
├── docker-compose.yml              # Multi-container orchestration (TimescaleDB, PostgreSQL, Grafana, Runner)
├── benchmark.py                    # Complete Python benchmarking suite (Ingestion, Storage, Querying)
├── requirements.txt                # Python dependencies (psycopg2-binary, numpy, etc.)
├── Dockerfile                      # Containerized benchmark harness for zero-setup execution
├── run_benchmark.ps1               # Automated 1-click execution script for Windows PowerShell
├── run_benchmark.sh                # Automated 1-click execution script for Linux/macOS
├── grafana/
│   ├── provisioning/
│   │   ├── datasources/
│   │   │   └── datasources.yml     # Automated datasource connection setup
│   │   └── dashboards/
│   │       └── dashboards.yml      # Automated dashboard provider registration
│   └── dashboards/
│       └── iot_temperature_dashboard.json # Rich 10-panel research telemetry dashboard
└── README.md                       # Comprehensive technical documentation
```

---

## 3. Quick Start & Execution

### Option A: Fully Automated Containerized Execution (Recommended)
This approach spins up both databases and runs the entire benchmarking suite inside Docker without needing Python or psycopg2 locally:

**On Windows (PowerShell):**
```powershell
.\run_benchmark.ps1
```

**On Linux / macOS (Bash):**
```bash
chmod +x run_benchmark.sh
./run_benchmark.sh
```

### Option B: Native Host Execution
If you prefer running `benchmark.py` directly on your host machine:

1. **Spin up database and Grafana services:**
   ```bash
   docker compose up -d timescaledb postgres_plain grafana
   ```

2. **Install host Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Run the benchmark:**
   ```bash
   python benchmark.py --host-ts localhost --port-ts 5432 --host-pg localhost --port-pg 5433
   ```

---

## 4. Evaluated Dimensions & Expected Findings

1. **Ingestion Throughput (Tuples/sec):**
   - As traditional PostgreSQL B-Tree indexes exceed available cache (`shared_buffers`), leaf split costs and random I/O degrade ingestion speed.
   - TimescaleDB maintains steady ingestion by writing exclusively to the active chunk in RAM.

2. **Physical Storage Footprint (MB):**
   - Vanilla PostgreSQL stores uncompressed tuples with significant tuple header overhead and large index structures (~150–180 MB).
   - TimescaleDB native columnar compression uses **Gorilla XOR** for double-precision floats, **Delta-of-Delta** for timestamps, and **LZ4/dictionary** for metadata, achieving an **85–92% reduction** in storage (~15–25 MB).

3. **Query Latency for Hourly Averages (ms):**
   - Vanilla PostgreSQL executes sequential chunk/index scans and an in-memory hash aggregate over all 1.21 million rows (~400–900 ms).
   - TimescaleDB's **Continuous Aggregate** pre-computes hourly summaries into a materialized hypertable, reducing the scan to just $20 \times 168 = 3,360$ summary rows, resulting in sub-5ms latency (**80x–150x speedup**).

---

## 5. Grafana Real-Time Visualization

Once the benchmark finishes, navigate to:
- **URL:** [http://localhost:3000](http://localhost:3000)
- **Username:** `admin`
- **Password:** `admin`
- **Dashboard:** Under "IoT Telemetry" folder, open **"IoT Temperature Logger Benchmark & Continuous Aggregates"**.
