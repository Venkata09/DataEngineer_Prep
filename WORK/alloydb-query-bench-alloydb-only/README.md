
# AlloyDB / Postgres Query Benchmark Kit (Python, AlloyDB-only)

Benchmark a set of SQL queries (from a config file) against AlloyDB / Postgres and export results to CSV/Markdown. Optional sink: Google Sheets.

## Features
- Queries defined in **YAML** (no hard-coding) with per-query params and session settings.
- Warmup runs + N timed runs; wall-clock + `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` stats.
- Summary metrics: mean/median/p95/stddev, rows, planning/execution time, buffers.
- Exports to **CSV** and **Markdown** by default; optional **Google Sheets**.
- Safe defaults (statement timeout) and per-query session overrides.

## Quick Start

### 1) Python env
```bash
python -m venv .venv && source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2) Configure DB creds via env vars
```bash
export PG_HOST="127.0.0.1"
export PG_PORT="5432"
export PG_DB="postgres"
export PG_USER="postgres"
export PG_PASSWORD="postgres"
# Optional: SSL params if needed by AlloyDB:
# export PG_SSLMODE="require"
# export PG_SSLROOTCERT="/path/to/server-ca.pem"
```

### 3) Edit your queries in `config.example.yaml`
```bash
cp config.example.yaml config.yaml
# Update query files in ./queries and any parameters/session settings you want
```

### 4) Run the benchmark
```bash
python run_bench.py --config config.yaml
```
Outputs are written to `./out/`:
- `benchmark_results_<timestamp>.csv`
- `benchmark_aggregates_<timestamp>.csv`
- `benchmark_summary_<timestamp>.md`

### Optional: Google Sheets
- Create a Service Account in Google Cloud, enable Google Sheets API.
- Share your Sheet with the SA email.
- Set env vars:
```bash
export GSHEET_ID="<your_sheet_id>"
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service_account.json"
```
The reporter will create/replace a sheet tab named `bench_<timestamp>`.

## Config format (`config.example.yaml`)
```yaml
run:
  repeats: 5           # number of timed runs per query
  warmups: 2           # warmup runs per query (not recorded)
  statement_timeout_ms: 60000  # per-session statement timeout
  default_session:
    # any SETs you want globally for all queries
    # work_mem: "64MB"
    # enable_nestloop: "on"

queries:
  - name: top_customers_last_30d
    file: queries/top_customers.sql
    params:
      days: 30
    session:
      # per-query overrides
      # enable_seqscan: "off"

  - name: count_orders
    file: queries/count_orders.sql

  - name: inline_demo
    sql: |
      SELECT generate_series(1, :n) AS n
    params:
      n: 100000
```

## Notes
- Requires `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` support (Postgres 9.0+). AlloyDB supports this.
- For best apples-to-apples comparisons, run with stable load, same search_path, and `ANALYZE` tables first.
- Consider enabling `pg_stat_statements` on the DB for additional insights.
- You can pin query planner choices via `session` → SETs to test alternatives (e.g., join strategies).
