#!/usr/bin/env python3
"""
================================================================================
DBMS Research Project: Time-Series Databases for IoT
Implementation & Benchmarking Suite (Role: S3 - Implementation Lead)
================================================================================
Description:
    Exhaustive, production-ready benchmarking harness evaluating TimescaleDB vs.
    vanilla PostgreSQL for high-frequency IoT telemetry.

Simulation Profile:
    - 20 IoT Temperature & Environmental Sensor Nodes
    - 1-Week continuous temporal horizon (604,800 seconds)
    - 10-second sampling resolution (60,480 samples/sensor)
    - Total Telemetry Volume: 1,209,600 tuples (~1.21 Million rows)

Evaluated Dimensions:
    1. Ingestion Throughput (tuples/sec, batch latency percentiles)
    2. Physical Storage Footprint (Heap vs. B-Tree Indexes vs. Chunks vs. Compressed)
    3. Query Latency & Speedup (Continuous Aggregates vs. date_trunc / time_bucket)

Author: S3 (Implementation Lead)
Collaborators: S1 (Lead/Editor), S2 (Literature Lead), S4 (Analysis Lead)
================================================================================
"""

import sys
import os
import time
import math
import random
import argparse
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Dict, Any, Generator

try:
    import psycopg2
    from psycopg2 import sql
    from psycopg2.extras import execute_values
except ImportError:
    print("CRITICAL ERROR: 'psycopg2' is not installed. Run: pip install psycopg2-binary")
    sys.exit(1)

# Configure high-precision logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (S3:BenchSuite) %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("IoT-TimeSeries-Bench")

# ------------------------------------------------------------------------------
# CONFIGURATION DEFAULTS
# ------------------------------------------------------------------------------
DEFAULT_SENSORS = 20
DEFAULT_SAMPLING_SECONDS = 10
DEFAULT_DURATION_DAYS = 7
DEFAULT_BATCH_SIZE = 10000
DEFAULT_QUERY_WARMUP = 5
DEFAULT_QUERY_RUNS = 30
DEFAULT_CHUNK_INTERVAL = "1 day"

# Synthetic Simulation Base Coordinates
SIMULATION_START = datetime(2026, 3, 1, 0, 0, 0, tzinfo=timezone.utc)

# ------------------------------------------------------------------------------
# SYNTHETIC DATA GENERATION ENGINE
# ------------------------------------------------------------------------------
class SensorProfile:
    """Mathematical parameterization of an IoT sensor node."""
    def __init__(self, sensor_id: int):
        self.sensor_id = sensor_id
        # Ambient baseline temperature: 18.0°C to 26.0°C
        self.temp_base = 18.0 + (sensor_id * 0.4)
        # Diurnal thermal amplitude: 3.5°C to 6.5°C
        self.temp_amplitude = 3.5 + ((sensor_id % 5) * 0.6)
        # Phase shift: sensor location disparity (-pi/4 to +pi/4)
        self.phase_shift = (sensor_id % 7 - 3) * (math.pi / 12)
        # Baseline humidity (inversely correlated): 45% to 75%
        self.humidity_base = 75.0 - (sensor_id * 1.2)
        # Battery degradation rate (loss over 1 week): 4.0% to 12.0%
        self.battery_decay_total = 4.0 + (sensor_id * 0.4)


def generate_telemetry_batch(
    profiles: List[SensorProfile],
    start_second: int,
    end_second: int,
    step_seconds: int
) -> List[Tuple[datetime, int, float, float, float]]:
    """
    Generates a vectorized/streamed batch of realistic IoT sensor observations.
    
    Equations:
        T(t) = T_base + A_temp * sin(2*pi*(t - 21600)/86400 + phi) + N(0, 0.25^2)
        H(t) = H_base - 0.7 * (T(t) - T_base) + N(0, 0.5^2)
        B(t) = 100.0 - (t / TotalDuration) * DecayTotal - LoadNoise
    """
    batch = []
    total_horizon_seconds = DEFAULT_DURATION_DAYS * 86400

    for current_sec in range(start_second, end_second, step_seconds):
        current_time = SIMULATION_START + timedelta(seconds=current_sec)
        diurnal_rad = (2.0 * math.pi * (current_sec - 21600) / 86400.0) # Peak at ~14:00 (2pm)
        decay_ratio = current_sec / total_horizon_seconds

        for profile in profiles:
            # Temperature model with Gaussian noise
            t_sin = math.sin(diurnal_rad + profile.phase_shift)
            noise_temp = random.gauss(0.0, 0.25)
            temperature = round(profile.temp_base + (profile.temp_amplitude * t_sin) + noise_temp, 3)

            # Humidity model (inverse correlation with temperature)
            noise_hum = random.gauss(0.0, 0.4)
            humidity = round(profile.humidity_base - (0.8 * (temperature - profile.temp_base)) + noise_hum, 2)
            humidity = max(10.0, min(99.0, humidity))

            # Battery discharge curve
            noise_bat = random.uniform(-0.05, 0.05)
            battery = round(100.0 - (decay_ratio * profile.battery_decay_total) + noise_bat, 2)
            battery = max(0.0, min(100.0, battery))

            batch.append((current_time, profile.sensor_id, temperature, humidity, battery))

    return batch


def stream_telemetry_batches(
    num_sensors: int,
    duration_days: int,
    step_seconds: int,
    batch_size: int
) -> Generator[List[Tuple[datetime, int, float, float, float]], None, None]:
    """Yields batches of tuples strictly sized to `batch_size`."""
    profiles = [SensorProfile(i) for i in range(1, num_sensors + 1)]
    total_seconds = duration_days * 86400
    rows_per_timestamp = num_sensors
    total_timestamps = total_seconds // step_seconds
    total_rows = total_timestamps * rows_per_timestamp

    logger.info("Initializing Generator: %d sensors, %d total seconds, %d rows",
                num_sensors, total_seconds, total_rows)

    accumulated: List[Tuple[datetime, int, float, float, float]] = []
    
    for sec in range(0, total_seconds, step_seconds):
        rows = generate_telemetry_batch(profiles, sec, sec + step_seconds, step_seconds)
        accumulated.extend(rows)
        while len(accumulated) >= batch_size:
            yield accumulated[:batch_size]
            accumulated = accumulated[batch_size:]

    if accumulated:
        yield accumulated


# ------------------------------------------------------------------------------
# DATABASE MANAGEMENT & SCHEMA INITIALIZATION
# ------------------------------------------------------------------------------
class BenchmarkDatabaseManager:
    """Manages connections, schema definitions, and DDL for TimescaleDB & PostgreSQL."""
    def __init__(self, conn_str_ts: str, conn_str_pg: str = None):
        self.conn_str_ts = conn_str_ts
        self.conn_str_pg = conn_str_pg or conn_str_ts # Fallback to single database if separate PG not supplied
        self.conn_ts = None
        self.conn_pg = None

    def connect(self):
        """Establishes connections to TimescaleDB and target PostgreSQL."""
        logger.info("Connecting to TimescaleDB instance: %s", self.conn_str_ts)
        self.conn_ts = psycopg2.connect(self.conn_str_ts)
        self.conn_ts.autocommit = True

        logger.info("Connecting to PostgreSQL baseline instance: %s", self.conn_str_pg)
        self.conn_pg = psycopg2.connect(self.conn_str_pg)
        self.conn_pg.autocommit = True

    def close(self):
        """Closes all active database sessions."""
        if self.conn_ts and not self.conn_ts.closed:
            self.conn_ts.close()
        if self.conn_pg and not self.conn_pg.closed:
            self.conn_pg.close()
        logger.info("All database connections gracefully terminated.")

    def initialize_timescaledb_schema(self, chunk_interval: str = DEFAULT_CHUNK_INTERVAL):
        """Prepares TimescaleDB extension, hypertable, indexes, and continuous aggregate."""
        with self.conn_ts.cursor() as cur:
            logger.info("Ensuring TimescaleDB extension is active...")
            cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")

            logger.info("Purging stale TimescaleDB relations...")
            cur.execute("DROP MATERIALIZED VIEW IF EXISTS sensor_hourly_cagg CASCADE;")
            cur.execute("DROP TABLE IF EXISTS sensor_data_ts CASCADE;")

            logger.info("Creating primary hypertable table: 'sensor_data_ts'...")
            cur.execute("""
                CREATE TABLE sensor_data_ts (
                    time TIMESTAMPTZ NOT NULL,
                    sensor_id INTEGER NOT NULL,
                    temperature DOUBLE PRECISION NOT NULL,
                    humidity DOUBLE PRECISION NOT NULL,
                    battery_level DOUBLE PRECISION NOT NULL
                );
            """)

            logger.info("Transforming 'sensor_data_ts' into hypertable (chunk_time_interval => '%s')...", chunk_interval)
            cur.execute(f"SELECT create_hypertable('sensor_data_ts', 'time', chunk_time_interval => INTERVAL '{chunk_interval}');")

            logger.info("Creating multi-column index on hypertable (sensor_id, time DESC)...")
            cur.execute("CREATE INDEX idx_sensor_ts_sensor_time ON sensor_data_ts (sensor_id, time DESC);")

            logger.info("Constructing Continuous Aggregate materialized view: 'sensor_hourly_cagg'...")
            cur.execute("""
                CREATE MATERIALIZED VIEW sensor_hourly_cagg
                WITH (timescaledb.continuous) AS
                SELECT
                    sensor_id,
                    time_bucket('1 hour', time) AS bucket,
                    AVG(temperature) AS avg_temp,
                    MIN(temperature) AS min_temp,
                    MAX(temperature) AS max_temp,
                    AVG(humidity) AS avg_humidity,
                    COUNT(*) AS sample_count
                FROM sensor_data_ts
                GROUP BY sensor_id, bucket
                WITH NO DATA;
            """)

            logger.info("Configuring TimescaleDB native columnar compression policy...")
            cur.execute("""
                ALTER TABLE sensor_data_ts SET (
                    timescaledb.compress,
                    timescaledb.compress_segmentby = 'sensor_id',
                    timescaledb.compress_orderby = 'time DESC'
                );
            """)

    def initialize_plain_postgres_schema(self):
        """Prepares standard relational table with traditional B-Tree index."""
        with self.conn_pg.cursor() as cur:
            logger.info("Purging stale plain PostgreSQL relations...")
            cur.execute("DROP TABLE IF EXISTS sensor_data_plain CASCADE;")

            logger.info("Creating standard heap table: 'sensor_data_plain'...")
            cur.execute("""
                CREATE TABLE sensor_data_plain (
                    time TIMESTAMPTZ NOT NULL,
                    sensor_id INTEGER NOT NULL,
                    temperature DOUBLE PRECISION NOT NULL,
                    humidity DOUBLE PRECISION NOT NULL,
                    battery_level DOUBLE PRECISION NOT NULL
                );
            """)

            logger.info("Building standard B-Tree index on 'time'...")
            cur.execute("CREATE INDEX idx_sensor_plain_time ON sensor_data_plain (time);")

            logger.info("Building secondary B-Tree composite index on (sensor_id, time DESC)...")
            cur.execute("CREATE INDEX idx_sensor_plain_sensor_time ON sensor_data_plain (sensor_id, time DESC);")

    def refresh_continuous_aggregate(self):
        """Manually materializes the entire simulated week in the Continuous Aggregate view."""
        with self.conn_ts.cursor() as cur:
            logger.info("Refreshing Continuous Aggregate 'sensor_hourly_cagg' over full temporal window...")
            start_refresh = time.perf_counter()
            cur.execute("CALL refresh_continuous_aggregate('sensor_hourly_cagg', NULL, NULL);")
            duration = time.perf_counter() - start_refresh
            logger.info("Continuous Aggregate refresh materialized in %.3f seconds.", duration)
            return duration

    def compress_hypertable_chunks(self):
        """Compresses all eligible chunks using TimescaleDB Gorilla/Delta-of-Delta compression."""
        with self.conn_ts.cursor() as cur:
            logger.info("Triggering manual chunk compression across hypertable...")
            start_compress = time.perf_counter()
            cur.execute("SELECT compress_chunk(c, if_not_compressed => true) FROM show_chunks('sensor_data_ts') c;")
            compressed_chunks = cur.fetchall()
            duration = time.perf_counter() - start_compress
            logger.info("Compressed %d chunks in %.3f seconds.", len(compressed_chunks), duration)
            return len(compressed_chunks), duration


# ------------------------------------------------------------------------------
# BENCHMARK SUITE: INSERT THROUGHPUT EVALUATOR
# ------------------------------------------------------------------------------
def benchmark_bulk_ingestion(
    conn,
    table_name: str,
    num_sensors: int,
    duration_days: int,
    step_seconds: int,
    batch_size: int
) -> Dict[str, Any]:
    """
    Executes and benchmarks bulk streaming ingestion using psycopg2.extras.execute_values.
    
    Metrics:
        - Total Elapsed Ingestion Time (s)
        - Average Ingestion Rate (rows/s)
        - Batch Latency Profile (Mean, P50, P95, Max)
    """
    logger.info("Starting Bulk Ingestion Benchmark on [%s]...", table_name)
    total_rows = (duration_days * 86400 // step_seconds) * num_sensors
    batch_latencies = []
    inserted_rows = 0

    insert_query = f"""
        INSERT INTO {table_name} (time, sensor_id, temperature, humidity, battery_level)
        VALUES %s;
    """

    stream = stream_telemetry_batches(
        num_sensors=num_sensors,
        duration_days=duration_days,
        step_seconds=step_seconds,
        batch_size=batch_size
    )

    t_start_total = time.perf_counter()
    with conn.cursor() as cur:
        for batch_idx, batch in enumerate(stream, start=1):
            t_batch_start = time.perf_counter()
            execute_values(cur, insert_query, batch, page_size=batch_size)
            t_batch_elapsed = time.perf_counter() - t_batch_start

            batch_latencies.append(t_batch_elapsed)
            inserted_rows += len(batch)

            if batch_idx % 20 == 0 or inserted_rows >= total_rows:
                curr_rate = inserted_rows / (time.perf_counter() - t_start_total)
                logger.info("Ingesting [%s]: %7d / %7d rows (%.1f%%) | Rate: %7.1f rows/sec | Last Batch: %.3fs",
                            table_name, inserted_rows, total_rows,
                            (inserted_rows / total_rows) * 100, curr_rate, t_batch_elapsed)

    t_total_elapsed = time.perf_counter() - t_start_total
    avg_throughput = inserted_rows / t_total_elapsed

    batch_latencies.sort()
    p50_latency = batch_latencies[len(batch_latencies) // 2]
    p95_latency = batch_latencies[int(len(batch_latencies) * 0.95)]
    p99_latency = batch_latencies[int(len(batch_latencies) * 0.99)]

    logger.info("COMPLETED [%s]: %d rows in %.3fs | Avg Rate: %.2f rows/sec",
                table_name, inserted_rows, t_total_elapsed, avg_throughput)

    return {
        "table": table_name,
        "total_rows": inserted_rows,
        "elapsed_seconds": t_total_elapsed,
        "throughput_rows_per_sec": avg_throughput,
        "batch_latency_mean_sec": sum(batch_latencies) / len(batch_latencies),
        "batch_latency_p50_sec": p50_latency,
        "batch_latency_p95_sec": p95_latency,
        "batch_latency_p99_sec": p99_latency,
        "batch_latency_max_sec": max(batch_latencies)
    }


# ------------------------------------------------------------------------------
# BENCHMARK SUITE: STORAGE FOOTPRINT ANALYZER
# ------------------------------------------------------------------------------
def benchmark_storage_footprint(db_manager: BenchmarkDatabaseManager) -> Dict[str, Any]:
    """
    Gathers low-level physical disk footprint for tables, B-Tree indexes,
    chunks, compression layers, and continuous aggregate views.
    """
    logger.info("Quantifying physical storage footprints...")
    metrics: Dict[str, Any] = {}

    # 1. Plain PostgreSQL Metrics
    with db_manager.conn_pg.cursor() as cur:
        cur.execute("SELECT pg_total_relation_size('sensor_data_plain');")
        plain_total = cur.fetchone()[0]

        cur.execute("SELECT pg_relation_size('sensor_data_plain');")
        plain_heap = cur.fetchone()[0]

        cur.execute("SELECT pg_indexes_size('sensor_data_plain');")
        plain_indexes = cur.fetchone()[0]

        metrics["plain_postgres"] = {
            "total_bytes": plain_total,
            "heap_bytes": plain_heap,
            "indexes_bytes": plain_indexes,
            "total_mb": round(plain_total / (1024 * 1024), 2),
            "heap_mb": round(plain_heap / (1024 * 1024), 2),
            "indexes_mb": round(plain_indexes / (1024 * 1024), 2)
        }

    # 2. TimescaleDB Uncompressed Hypertable & CAGG Metrics
    with db_manager.conn_ts.cursor() as cur:
        cur.execute("SELECT pg_total_relation_size('sensor_data_ts');")
        ts_total_uncompressed = cur.fetchone()[0]

        cur.execute("SELECT pg_relation_size('sensor_data_ts');")
        ts_table_uncompressed = cur.fetchone()[0]

        cur.execute("SELECT pg_indexes_size('sensor_data_ts');")
        ts_indexes_uncompressed = cur.fetchone()[0]

        # Chunk Count
        cur.execute("SELECT count(*) FROM show_chunks('sensor_data_ts');")
        chunk_count = cur.fetchone()[0]

        # Continuous Aggregate Materialized Table Size
        cur.execute("""
            SELECT COALESCE(pg_total_relation_size(format('%I.%I', view_schema, view_name)::regclass), 0)
            FROM timescaledb_information.continuous_aggregates
            WHERE view_name = 'sensor_hourly_cagg';
        """)
        cagg_total = cur.fetchone()[0]

        metrics["timescaledb_uncompressed"] = {
            "total_bytes": ts_total_uncompressed,
            "heap_bytes": ts_table_uncompressed,
            "indexes_bytes": ts_indexes_uncompressed,
            "chunk_count": chunk_count,
            "total_mb": round(ts_total_uncompressed / (1024 * 1024), 2),
            "heap_mb": round(ts_table_uncompressed / (1024 * 1024), 2),
            "indexes_mb": round(ts_indexes_uncompressed / (1024 * 1024), 2),
            "cagg_bytes": cagg_total,
            "cagg_mb": round(cagg_total / (1024 * 1024), 3)
        }

    # 3. TimescaleDB Compressed Footprint
    logger.info("Executing TimescaleDB native compression on hypertable chunks...")
    chunks_compressed, compress_duration = db_manager.compress_hypertable_chunks()

    with db_manager.conn_ts.cursor() as cur:
        cur.execute("SELECT pg_total_relation_size('sensor_data_ts');")
        ts_total_compressed = cur.fetchone()[0]

        unc_total = ts_total_uncompressed
        comp_total = ts_total_compressed

        # Retrieve compression statistics via TimescaleDB 2.x standard functions
        try:
            cur.execute("""
                SELECT
                    COALESCE(before_compression_total_bytes, 0),
                    COALESCE(after_compression_total_bytes, 0)
                FROM hypertable_compression_stats('sensor_data_ts');
            """)
            row = cur.fetchone()
            if row and row[0] and row[1]:
                unc_total = row[0]
                comp_total = row[1]
        except Exception as e:
            logger.warning("hypertable_compression_stats query encountered: %s. Using relation size fallback.", e)
            unc_total = ts_total_uncompressed
            comp_total = ts_total_compressed

        comp_ratio = (unc_total / comp_total) if comp_total > 0 else 1.0
        space_saving_pct = (1.0 - (comp_total / unc_total)) * 100.0 if unc_total > 0 else 0.0

        metrics["timescaledb_compressed"] = {
            "total_bytes": ts_total_compressed,
            "total_mb": round(ts_total_compressed / (1024 * 1024), 2),
            "uncompressed_total_bytes": unc_total,
            "compressed_total_bytes": comp_total,
            "compression_ratio": round(comp_ratio, 2),
            "space_saving_percent": round(space_saving_pct, 2),
            "chunks_compressed": chunks_compressed,
            "compress_duration_sec": round(compress_duration, 3)
        }

    return metrics


# ------------------------------------------------------------------------------
# BENCHMARK SUITE: QUERY LATENCY EVALUATOR
# ------------------------------------------------------------------------------
def execute_timed_query(conn, query_sql: str, runs: int, warmup: int) -> Dict[str, Any]:
    """
    Executes warmups followed by statistical measurement runs of a targeted SQL query.
    Calculates Mean, Median, Std Dev, Min, Max, P95, P99.
    """
    latencies_ms = []

    with conn.cursor() as cur:
        # Warmup loop to eliminate buffer cache / OS page cold-start anomalies
        for _ in range(warmup):
            cur.execute(query_sql)
            cur.fetchall()

        # Measurement loop
        for _ in range(runs):
            t_start = time.perf_counter()
            cur.execute(query_sql)
            rows = cur.fetchall()
            t_elapsed_ms = (time.perf_counter() - t_start) * 1000.0
            latencies_ms.append(t_elapsed_ms)

        # Retrieve EXPLAIN (ANALYZE, BUFFERS)
        explain_plan = "N/A"
        try:
            cur.execute(f"EXPLAIN (ANALYZE, BUFFERS) {query_sql}")
            explain_plan = "\n".join([row[0] for row in cur.fetchall()])
        except Exception as e:
            logger.warning("EXPLAIN (ANALYZE, BUFFERS) query warning: %s", e)

    latencies_ms.sort()
    n = len(latencies_ms)
    mean_val = sum(latencies_ms) / n
    median_val = latencies_ms[n // 2]
    variance = sum((x - mean_val) ** 2 for x in latencies_ms) / (n - 1) if n > 1 else 0.0
    std_dev = math.sqrt(variance)

    return {
        "runs": runs,
        "row_count": len(rows),
        "mean_ms": round(mean_val, 3),
        "median_ms": round(median_val, 3),
        "std_dev_ms": round(std_dev, 3),
        "min_ms": round(min(latencies_ms), 3),
        "max_ms": round(max(latencies_ms), 3),
        "p95_ms": round(latencies_ms[int(n * 0.95)], 3),
        "p99_ms": round(latencies_ms[int(n * 0.99)], 3),
        "explain_summary": explain_plan.splitlines()[0] if explain_plan else "N/A",
        "full_explain_plan": explain_plan
    }


def benchmark_query_performance(
    db_manager: BenchmarkDatabaseManager,
    warmup: int = DEFAULT_QUERY_WARMUP,
    runs: int = DEFAULT_QUERY_RUNS
) -> Dict[str, Any]:
    """
    Executes the analytical query benchmarks comparing:
    1. Vanilla PostgreSQL: date_trunc('hour', time) with full GROUP BY
    2. TimescaleDB Raw Hypertable: time_bucket('1 hour', time) with GROUP BY
    3. TimescaleDB Continuous Aggregate: pre-materialized hourly view
    """
    logger.info("Commencing Analytical Query Latency Benchmark (%d runs, %d warmups)...", runs, warmup)

    # -------------------------------------------------------------------------
    # Q1: Plain PostgreSQL date_trunc('hour', time) over full 1-week horizon
    # -------------------------------------------------------------------------
    q1_sql = """
        SELECT
            sensor_id,
            date_trunc('hour', time) AS bucket,
            AVG(temperature) AS avg_temp,
            MIN(temperature) AS min_temp,
            MAX(temperature) AS max_temp,
            AVG(humidity) AS avg_humidity,
            COUNT(*) AS sample_count
        FROM sensor_data_plain
        GROUP BY sensor_id, bucket
        ORDER BY sensor_id, bucket;
    """
    logger.info("Executing Q1: Plain PostgreSQL date_trunc('hour', time)...")
    res_q1 = execute_timed_query(db_manager.conn_pg, q1_sql, runs=runs, warmup=warmup)

    # -------------------------------------------------------------------------
    # Q2: TimescaleDB Raw Hypertable time_bucket('1 hour', time)
    # -------------------------------------------------------------------------
    q2_sql = """
        SELECT
            sensor_id,
            time_bucket('1 hour', time) AS bucket,
            AVG(temperature) AS avg_temp,
            MIN(temperature) AS min_temp,
            MAX(temperature) AS max_temp,
            AVG(humidity) AS avg_humidity,
            COUNT(*) AS sample_count
        FROM sensor_data_ts
        GROUP BY sensor_id, bucket
        ORDER BY sensor_id, bucket;
    """
    logger.info("Executing Q2: TimescaleDB Hypertable time_bucket('1 hour', time)...")
    res_q2 = execute_timed_query(db_manager.conn_ts, q2_sql, runs=runs, warmup=warmup)

    # -------------------------------------------------------------------------
    # Q3: TimescaleDB Continuous Aggregate (sensor_hourly_cagg)
    # -------------------------------------------------------------------------
    q3_sql = """
        SELECT
            sensor_id,
            bucket,
            avg_temp,
            min_temp,
            max_temp,
            avg_humidity,
            sample_count
        FROM sensor_hourly_cagg
        ORDER BY sensor_id, bucket;
    """
    logger.info("Executing Q3: TimescaleDB Continuous Aggregate materialized query...")
    res_q3 = execute_timed_query(db_manager.conn_ts, q3_sql, runs=runs, warmup=warmup)

    # -------------------------------------------------------------------------
    # Q4: Filtered Single Sensor Query (Plain vs CAGG)
    # -------------------------------------------------------------------------
    q4_plain_sql = """
        SELECT
            date_trunc('hour', time) AS bucket,
            AVG(temperature) AS avg_temp,
            MIN(temperature) AS min_temp,
            MAX(temperature) AS max_temp
        FROM sensor_data_plain
        WHERE sensor_id = 7
        GROUP BY bucket
        ORDER BY bucket;
    """
    logger.info("Executing Q4: Single Sensor Filtered (Plain PostgreSQL)...")
    res_q4_plain = execute_timed_query(db_manager.conn_pg, q4_plain_sql, runs=runs, warmup=warmup)

    q4_cagg_sql = """
        SELECT
            bucket,
            avg_temp,
            min_temp,
            max_temp
        FROM sensor_hourly_cagg
        WHERE sensor_id = 7
        ORDER BY bucket;
    """
    logger.info("Executing Q4: Single Sensor Filtered (TimescaleDB Continuous Aggregate)...")
    res_q4_cagg = execute_timed_query(db_manager.conn_ts, q4_cagg_sql, runs=runs, warmup=warmup)

    # Calculate comparative speedup ratios
    speedup_cagg_vs_plain = res_q1["mean_ms"] / res_q3["mean_ms"] if res_q3["mean_ms"] > 0 else 0.0
    speedup_cagg_vs_hyper = res_q2["mean_ms"] / res_q3["mean_ms"] if res_q3["mean_ms"] > 0 else 0.0
    speedup_filtered = res_q4_plain["mean_ms"] / res_q4_cagg["mean_ms"] if res_q4_cagg["mean_ms"] > 0 else 0.0

    return {
        "Q1_Plain_date_trunc": res_q1,
        "Q2_Timescale_time_bucket": res_q2,
        "Q3_Timescale_Continuous_Aggregate": res_q3,
        "Q4_Single_Sensor_Plain": res_q4_plain,
        "Q4_Single_Sensor_CAGG": res_q4_cagg,
        "speedup_cagg_vs_plain": round(speedup_cagg_vs_plain, 2),
        "speedup_cagg_vs_hypertable": round(speedup_cagg_vs_hyper, 2),
        "speedup_single_sensor_cagg_vs_plain": round(speedup_filtered, 2)
    }


# ------------------------------------------------------------------------------
# REPORT GENERATOR & TERMINAL FORMATTER
# ------------------------------------------------------------------------------
def print_and_export_results(
    insert_plain: Dict[str, Any],
    insert_ts: Dict[str, Any],
    storage_metrics: Dict[str, Any],
    query_metrics: Dict[str, Any],
    output_dir: str
):
    """Prints publication-grade tabular ASCII reports and writes JSON & CSV summaries."""
    os.makedirs(output_dir, exist_ok=True)

    header = "=" * 88
    sub_header = "-" * 88

    print("\n" + header)
    print("                     DBMS RESEARCH BENCHMARK: EXPERIMENTAL RESULTS")
    print("              TimescaleDB vs. Standard PostgreSQL for 20 IoT Sensors (1 Week)")
    print(header)

    # 1. Ingestion Table
    print("\n[A] INGESTION THROUGHPUT & BATCH LATENCY COMPARISON")
    print(sub_header)
    print(f"{'Target Database / Engine':<32} | {'Throughput (rows/s)':<20} | {'Total Time (s)':<14} | {'P95 Latency (s)':<14}")
    print(sub_header)
    print(f"{'PostgreSQL 16 (B-Tree Index)':<32} | {insert_plain['throughput_rows_per_sec']:<20.1f} | {insert_plain['elapsed_seconds']:<14.2f} | {insert_plain['batch_latency_p95_sec']:<14.3f}")
    print(f"{'TimescaleDB Hypertable':<32} | {insert_ts['throughput_rows_per_sec']:<20.1f} | {insert_ts['elapsed_seconds']:<14.2f} | {insert_ts['batch_latency_p95_sec']:<14.3f}")
    throughput_gain = ((insert_ts['throughput_rows_per_sec'] / insert_plain['throughput_rows_per_sec']) - 1.0) * 100
    print(sub_header)
    print(f"Ingestion Throughput Differential: {throughput_gain:+.2f}% on TimescaleDB\n")

    # 2. Storage Footprint Table
    print("[B] PHYSICAL DISK FOOTPRINT ANALYSIS")
    print(sub_header)
    print(f"{'Storage Representation':<36} | {'Heap (MB)':<12} | {'Indexes (MB)':<12} | {'Total Disk (MB)':<16} | {'Footprint (%)':<12}")
    print(sub_header)
    plain_mb = storage_metrics["plain_postgres"]["total_mb"]
    print(f"{'Vanilla PostgreSQL (Heap + B-Trees)':<36} | {storage_metrics['plain_postgres']['heap_mb']:<12.2f} | {storage_metrics['plain_postgres']['indexes_mb']:<12.2f} | {plain_mb:<16.2f} | 100.00%")
    
    unc_mb = storage_metrics["timescaledb_uncompressed"]["total_mb"]
    unc_ratio = (unc_mb / plain_mb) * 100
    print(f"{'TimescaleDB Uncompressed Hypertable':<36} | {storage_metrics['timescaledb_uncompressed']['heap_mb']:<12.2f} | {storage_metrics['timescaledb_uncompressed']['indexes_mb']:<12.2f} | {unc_mb:<16.2f} | {unc_ratio:.2f}%")

    comp_mb = storage_metrics["timescaledb_compressed"]["total_mb"]
    comp_ratio = (comp_mb / plain_mb) * 100
    print(f"{'TimescaleDB Compressed (Gorilla/LZ4)':<36} | {'--':<12} | {'--':<12} | {comp_mb:<16.2f} | {comp_ratio:.2f}%")

    cagg_mb = storage_metrics["timescaledb_uncompressed"]["cagg_mb"]
    print(f"{'Hourly Continuous Aggregate View':<36} | {'--':<12} | {'--':<12} | {cagg_mb:<16.2f} | {(cagg_mb/plain_mb)*100:.2f}%")
    print(sub_header)
    print(f"TimescaleDB Columnar Compression Ratio: {storage_metrics['timescaledb_compressed']['compression_ratio']}x ({storage_metrics['timescaledb_compressed']['space_saving_percent']}% space reduction)\n")

    # 3. Query Latency Table
    print("[C] HOURLY AGGREGATE QUERY LATENCY (1-WEEK DATASET)")
    print(sub_header)
    print(f"{'Query Strategy / Paradigm':<36} | {'Mean Latency':<14} | {'Median Latency':<14} | {'P95 Latency':<14} | {'Speedup':<10}")
    print(sub_header)
    q1 = query_metrics["Q1_Plain_date_trunc"]
    q2 = query_metrics["Q2_Timescale_time_bucket"]
    q3 = query_metrics["Q3_Timescale_Continuous_Aggregate"]

    print(f"{'Q1: Plain date_trunc (GROUP BY)':<36} | {q1['mean_ms']:<11.2f} ms | {q1['median_ms']:<11.2f} ms | {q1['p95_ms']:<11.2f} ms | 1.00x (ref)")
    print(f"{'Q2: Hypertable time_bucket':<36} | {q2['mean_ms']:<11.2f} ms | {q2['median_ms']:<11.2f} ms | {q2['p95_ms']:<11.2f} ms | {q1['mean_ms']/q2['mean_ms']:<9.2f}x")
    print(f"{'Q3: Timescale Continuous Aggregate':<36} | {q3['mean_ms']:<11.2f} ms | {q3['median_ms']:<11.2f} ms | {q3['p95_ms']:<11.2f} ms | {query_metrics['speedup_cagg_vs_plain']:<9.2f}x")
    print(sub_header)
    print(f"Continuous Aggregate Acceleration: {query_metrics['speedup_cagg_vs_plain']}x faster than vanilla PostgreSQL B-Tree scan!")
    print(header + "\n")

    # Export complete results to JSON
    export_payload = {
        "metadata": {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "sensors": DEFAULT_SENSORS,
            "duration_days": DEFAULT_DURATION_DAYS,
            "sampling_interval_seconds": DEFAULT_SAMPLING_SECONDS,
            "total_tuples": insert_ts["total_rows"]
        },
        "ingestion": {
            "plain_postgres": insert_plain,
            "timescaledb": insert_ts
        },
        "storage": storage_metrics,
        "query_performance": query_metrics
    }

    json_path = os.path.join(output_dir, "benchmark_results.json")
    with open(json_path, "w") as f:
        json.dump(export_payload, f, indent=2)
    logger.info("Serialized raw benchmark telemetry to: %s", json_path)

    # Export summary CSV for Phase 3 LaTeX publication pipeline
    csv_path = os.path.join(output_dir, "benchmark_summary.csv")
    with open(csv_path, "w") as f:
        f.write("metric_category,system,metric_name,value,unit\n")
        f.write(f"ingestion,plain_postgres,throughput,{insert_plain['throughput_rows_per_sec']},rows_sec\n")
        f.write(f"ingestion,timescaledb,throughput,{insert_ts['throughput_rows_per_sec']},rows_sec\n")
        f.write(f"storage,plain_postgres,total_disk,{storage_metrics['plain_postgres']['total_mb']},MB\n")
        f.write(f"storage,timescaledb_uncompressed,total_disk,{storage_metrics['timescaledb_uncompressed']['total_mb']},MB\n")
        f.write(f"storage,timescaledb_compressed,total_disk,{storage_metrics['timescaledb_compressed']['total_mb']},MB\n")
        f.write(f"storage,timescaledb_cagg,total_disk,{storage_metrics['timescaledb_uncompressed']['cagg_mb']},MB\n")
        f.write(f"query_latency,plain_date_trunc,mean_ms,{q1['mean_ms']},ms\n")
        f.write(f"query_latency,timescale_time_bucket,mean_ms,{q2['mean_ms']},ms\n")
        f.write(f"query_latency,timescale_cagg,mean_ms,{q3['mean_ms']},ms\n")
        f.write(f"query_speedup,cagg_vs_plain,factor,{query_metrics['speedup_cagg_vs_plain']},x\n")
    logger.info("Serialized tabular CSV summary to: %s", csv_path)


# ------------------------------------------------------------------------------
# CLI ARGUMENT PARSER & MAIN EXECUTION ORCHESTRATOR
# ------------------------------------------------------------------------------
def parse_arguments() -> argparse.Namespace:
    # Filter out accidental duplicate 'python' or 'benchmark.py' passed by container run wrappers
    clean_argv = []
    for arg in sys.argv[1:]:
        if arg == "python" or arg.endswith("benchmark.py"):
            continue
        clean_argv.append(arg)

    parser = argparse.ArgumentParser(
        description="DBMS Research Benchmarking Suite: TimescaleDB vs. PostgreSQL for IoT Telemetry",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    # Connection parameters
    parser.add_argument("--host-ts", default=os.getenv("TIMESCALE_HOST", "localhost"), help="TimescaleDB host")
    parser.add_argument("--port-ts", type=int, default=int(os.getenv("TIMESCALE_PORT", 5432)), help="TimescaleDB port")
    parser.add_argument("--user-ts", default=os.getenv("TIMESCALE_USER", "postgres"), help="TimescaleDB user")
    parser.add_argument("--pass-ts", default=os.getenv("TIMESCALE_PASSWORD", "postgrespassword"), help="TimescaleDB password")
    parser.add_argument("--db-ts", default=os.getenv("TIMESCALE_DB", "iot_timeseries_db"), help="TimescaleDB database name")

    parser.add_argument("--host-pg", default=os.getenv("POSTGRES_HOST", "localhost"), help="Plain PostgreSQL host")
    parser.add_argument("--port-pg", type=int, default=int(os.getenv("POSTGRES_PORT", 5433)), help="Plain PostgreSQL port")
    parser.add_argument("--user-pg", default=os.getenv("POSTGRES_USER", "postgres"), help="Plain PostgreSQL user")
    parser.add_argument("--pass-pg", default=os.getenv("POSTGRES_PASSWORD", "postgrespassword"), help="Plain PostgreSQL password")
    parser.add_argument("--db-pg", default=os.getenv("POSTGRES_DB", "iot_plain_db"), help="Plain PostgreSQL database name")

    # Simulation scaling parameters
    parser.add_argument("--sensors", type=int, default=DEFAULT_SENSORS, help="Number of simulated IoT sensor nodes")
    parser.add_argument("--days", type=int, default=DEFAULT_DURATION_DAYS, help="Duration of simulation horizon in days")
    parser.add_argument("--sampling-seconds", type=int, default=DEFAULT_SAMPLING_SECONDS, help="Sampling frequency in seconds")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Bulk insertion batch size")
    parser.add_argument("--chunk-interval", default=DEFAULT_CHUNK_INTERVAL, help="TimescaleDB hypertable chunk interval")
    parser.add_argument("--warmup-runs", type=int, default=DEFAULT_QUERY_WARMUP, help="Query warmup iterations")
    parser.add_argument("--query-runs", type=int, default=DEFAULT_QUERY_RUNS, help="Query measurement iterations")
    parser.add_argument("--output-dir", default="./benchmark_output", help="Directory for exported results")
    parser.add_argument("--dry-run-schema-only", action="store_true", help="Only build schemas without full ingest")

    return parser.parse_args(clean_argv)


def main():
    args = parse_arguments()

    conn_str_ts = f"host={args.host_ts} port={args.port_ts} user={args.user_ts} password={args.pass_ts} dbname={args.db_ts}"
    conn_str_pg = f"host={args.host_pg} port={args.port_pg} user={args.user_pg} password={args.pass_pg} dbname={args.db_pg}"

    logger.info("Initializing Research Benchmarking Pipeline (Role S3)...")
    logger.info("Simulating %d sensors over %d days at %d-sec frequency (~%d rows)...",
                args.sensors, args.days, args.sampling_seconds,
                (args.days * 86400 // args.sampling_seconds) * args.sensors)

    db_manager = BenchmarkDatabaseManager(conn_str_ts=conn_str_ts, conn_str_pg=conn_str_pg)

    try:
        db_manager.connect()

        # Step 1: DDL & Schema Setup
        logger.info("[PHASE 1.1] Initializing DDL schemas for TimescaleDB and Vanilla PostgreSQL...")
        db_manager.initialize_timescaledb_schema(chunk_interval=args.chunk_interval)
        db_manager.initialize_plain_postgres_schema()

        if args.dry_run_schema_only:
            logger.info("Dry-run requested. Schemas initialized successfully. Exiting.")
            return

        # Step 2: Ingestion Benchmarks
        logger.info("[PHASE 1.2] Executing Ingestion Benchmark: Plain PostgreSQL Baseline...")
        insert_plain = benchmark_bulk_ingestion(
            conn=db_manager.conn_pg,
            table_name="sensor_data_plain",
            num_sensors=args.sensors,
            duration_days=args.days,
            step_seconds=args.sampling_seconds,
            batch_size=args.batch_size
        )

        logger.info("[PHASE 1.2] Executing Ingestion Benchmark: TimescaleDB Hypertable...")
        insert_ts = benchmark_bulk_ingestion(
            conn=db_manager.conn_ts,
            table_name="sensor_data_ts",
            num_sensors=args.sensors,
            duration_days=args.days,
            step_seconds=args.sampling_seconds,
            batch_size=args.batch_size
        )

        # Step 3: Materialize Continuous Aggregates
        logger.info("[PHASE 1.3] Refreshing Continuous Aggregate Materialization...")
        db_manager.refresh_continuous_aggregate()

        # Step 4: Measure Storage Footprint (Before & After Compression)
        logger.info("[PHASE 1.4] Quantifying Physical Storage Footprint...")
        storage_metrics = benchmark_storage_footprint(db_manager)

        # Step 5: Query Latency Benchmark
        logger.info("[PHASE 1.5] Benchmarking Query Latencies for Hourly Temperature Aggregation...")
        query_metrics = benchmark_query_performance(
            db_manager=db_manager,
            warmup=args.warmup_runs,
            runs=args.query_runs
        )

        # Step 6: Presentation & Serialization
        logger.info("[PHASE 1.6] Generating Consolidated Research Report...")
        print_and_export_results(
            insert_plain=insert_plain,
            insert_ts=insert_ts,
            storage_metrics=storage_metrics,
            query_metrics=query_metrics,
            output_dir=args.output_dir
        )

    except Exception as e:
        logger.exception("An unhandled exception aborted the benchmark execution: %s", str(e))
        sys.exit(1)
    finally:
        db_manager.close()


if __name__ == "__main__":
    main()
