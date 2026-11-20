FROM python:3.11-slim

LABEL maintainer="DBMS Research Group <research@dbms-iot.edu>"
LABEL description="Containerized Benchmarking Harness for TimescaleDB vs PostgreSQL IoT Evaluation"

WORKDIR /workspace

# Install system dependencies required for psycopg2 build and performance measurement
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /workspace/
RUN pip install --no-cache-dir -r requirements.txt

COPY benchmark.py /workspace/

CMD ["python", "benchmark.py", "--host-ts", "timescaledb", "--host-pg", "postgres_plain", "--output-dir", "/workspace/benchmark_output"]
