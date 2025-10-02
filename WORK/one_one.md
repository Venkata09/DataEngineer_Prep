# Table Metrics – Design & Implementation Guide

This document explains the **normalized design** we’re using to capture per-table row counts on a schedule, plus how we materialize **run-level** and **daily** JSON snapshots for easy consumption by dashboards or downstream jobs.

---

## 1) Overview

* We run a Cloud Function (triggered daily, or on demand) that:

  1. discovers target tables,
  2. computes `COUNT(*)` for each,
  3. writes results into **normalized detail** tables (one row per table per run),
  4. writes two **snapshots**:

     * **Per-run** snapshot (append-only)
     * **Per-day** snapshot (one row per business day; “last successful run wins”).

* The request contract is **inverse**: we **scan everything by default** and the request can **exclude** specific tables and/or **skip** whole schemas.

---

## 2) Goals & Non-Goals

**Goals**

* Simple, auditable, and scalable storage of table row counts.
* Support multiple runs per day; keep an authoritative **daily** value without losing run history.
* Make reads easy for dashboards (one JSON blob per day or latest run).

**Non-Goals**

* Full data-profiling (null ratios, distributions, etc.).
* Cross-DB federation or non-Postgres dialects. (We target AlloyDB/Postgres.)
* Heavy analytics in the function. (We only compute counts.)

---

## 3) Architecture & Data Flow

1. **Trigger** (Cloud Scheduler → Pub/Sub → Eventarc → Cloud Function).

2. **Writer function** (CloudEvent):

   * Persists a **run header** (`RUNNING`).
   * Discovers tables; filters by exclusions.
   * Counts rows per table; persists **measurements**.
   * Builds nested JSON payload `{schema: {table: count}}`.
   * Writes **run snapshot** (append).
   * Writes **daily snapshot** (upsert; last-run-wins).
   * Updates **run header** to `SUCCEEDED` (or `FAILED` on errors).

3. **Consumers** (dashboards / jobs):

   * Read **daily snapshot** for a given date, or
   * Read **latest** from the **run snapshot**, or
   * Build ad-hoc analyses from **normalized detail** tables.

---

## 4) Data Model

### 4.1 Normalized core (source of truth)

* **`nms.table_metrics_runs`**
  One row per function execution (the “run”).

  * `run_id BIGSERIAL PK`
  * `collected_at timestamptz DEFAULT now()`
  * `snapshot_date date DEFAULT ((now() AT TIME ZONE 'America/New_York')::date)`
    *(Use your business TZ. If you prefer, drop this column and index an expression on `collected_at`.)*
  * `status text CHECK ... ('RUNNING','SUCCEEDED','FAILED')`
  * `error_message text`
  * `skip_schemas jsonb` (what we skipped, e.g., `["tmp","stage"]`)
  * `exclude_tables jsonb` (exact tables we excluded)

* **`nms.table_metrics_measurements`**
  One row **per table per run**.

  * `(run_id, schema_name, table_name) PK`
  * `row_count bigint`

**Why normalized?**
Trends, auditing, and corrections become trivial: you can filter by table/run/day, compare deltas, or regenerate snapshots.

### 4.2 Materialized snapshots (for fast, simple reads)

* **`nms.table_metrics_run_snapshot`** (append-only)

  * `run_id PK`
  * `payload jsonb` — nested `{schema: {table: count}}`
  * One row **per run**.

* **`nms.table_metrics_daily_snapshot`** (one row per business day)

  * `snapshot_date PK`
  * `run_id` (the run that produced this snapshot)
  * `payload jsonb`
  * **Upsert rule:** only overwrite if the incoming `run_id` is **newer** than the stored `run_id`.
    (Implements “**last successful run wins**” per day.)

---

## 5) Write Path (Cloud Function behavior)

**Input (CloudEvent → Pub/Sub message body JSON):**

```json
{
  "skipSchemas": ["tmp", "stage"],   // optional
  "exclude": ["public.orders"]       // optional
}
```

*(Empty `{}` scans everything except system schemas.)*

**Steps executed by the function:**

1. Insert a **RUNNING** row into `table_metrics_runs`; get `run_id`.
2. Query `information_schema.tables` to list user tables; drop system schemas and requested exclusions.
3. For each target table, run `SELECT COUNT(*)` and accumulate:

   * Detail rows → `table_metrics_measurements`
   * Nested JSON → `{"schema": {"table": count}}`
4. Write **run snapshot** → `table_metrics_run_snapshot` (upsert on `run_id`).
5. Write **daily snapshot** → `table_metrics_daily_snapshot` (upsert on `snapshot_date`, **only** if `incoming.run_id > existing.run_id`).
6. Update `table_metrics_runs.status` to **SUCCEEDED** (or **FAILED** with `error_message` on any exception).

**Why two snapshots?**

* **Run snapshot** preserves every execution’s output (forensics, comparisons).
* **Daily snapshot** offers a single stable record per business day (what most UIs want).

---

## 6) Read Path (what consumers query)

### Daily snapshot (one row per business day)

* **Today’s payload**

  ```sql
  SELECT payload
  FROM nms.table_metrics_daily_snapshot
  WHERE snapshot_date = CURRENT_DATE;
  ```
* **Specific day**

  ```sql
  SELECT payload
  FROM nms.table_metrics_daily_snapshot
  WHERE snapshot_date = DATE '2025-10-02';
  ```

### Latest run snapshot (most recent execution)

```sql
SELECT j.payload
FROM nms.table_metrics_run_snapshot j
JOIN nms.table_metrics_runs r USING (run_id)
ORDER BY r.collected_at DESC
LIMIT 1;
```

### Trends from normalized detail

* **Last 7 days for one table**

  ```sql
  SELECT r.snapshot_date, d.row_count
  FROM nms.table_metrics_runs r
  JOIN nms.table_metrics_measurements d USING (run_id)
  WHERE d.schema_name='public' AND d.table_name='orders'
    AND r.snapshot_date >= CURRENT_DATE - 7
  ORDER BY r.snapshot_date;
  ```

---

## 7) Deployment & Configuration

* Cloud Function (Gen2), Python 3.11
* Dependencies (examples):
  `sqlalchemy`, `pg8000`, `cloudevents`, `functions-framework`, `google-cloud-alloy-db-connector`
* Env vars:

  * `SNAPSHOT_TZ=America/New_York` (for logs/response; DB’s `snapshot_date` default is set in DDL)
* Scheduling:

  * Cloud Scheduler → Pub/Sub → Eventarc → this function
  * Message body: `{}`, or include `skipSchemas` / `exclude`

---

## 8) Error Handling & Observability

* Every execution inserts a **RUNNING** header first to get a `run_id`.
* On success, set to **SUCCEEDED**; on any unhandled exception, set **FAILED** with `error_message` (truncated).
* This guarantees an audit trail even for failed runs.
* Add log-based alerts for:

  * Repeated failures
  * No tables discovered
  * Significant spikes in row deltas (optional downstream rule)

---

## 9) Performance & Scaling

* For ~20–30 tables, `COUNT(*)` is fine. If a table grows large and counts become slow:

  * Use an **estimate** (e.g., `pg_stat_user_tables.n_live_tup`) for that specific table.
  * Or maintain a **materialized count** in the DB and read it.
* Discovery/query runs single-threaded by default. If needed:

  * Parallelize counts per schema (careful with connection limits).
* The normalized detail table grows linearly with **tables × runs** (manageable; index `(schema_name, table_name, run_id)` covers common queries).

---

## 10) Extensibility

* Add new metrics to `table_metrics_measurements`:

  * `bytes_total`, `bytes_table`, `bytes_index`, `vacuum_age`, etc.
* Add filtered counts (e.g., `row_count_active`) by adding new columns or a separate measurements type.
* Version the snapshot payload if you change shape:

  ```json
  { "_version": 2, "public": { "orders": {"count": 123, "bytes": 45678} } }
  ```

---

## 11) Security & Access

* Service account for the function should have:

  * **Read** on target database (to run `COUNT(*)` & `information_schema` queries).
  * **Write** on the `nms` schema.
* Lock down who can publish to the Pub/Sub topic.
* If exposing an HTTP reader (optional), require IAM auth.

---

## 12) Testing & Backfill

* **Dry run**: publish `{ "exclude": ["schema.table_that_causes_issues"] }` to validate exclusions.
* **Local**: use Functions Framework + a test CloudEvent payload.
* **Backfill**:

  * You can re-run the function for prior days by **overriding** at the DB layer (not via request) if you set `snapshot_date` manually during a controlled backfill script.
  * Alternatively, insert into `runs`/`measurements` directly and then regenerate snapshots:

    ```sql
    -- build daily snapshot from detail for a day
    WITH last_run AS (
      SELECT run_id
      FROM nms.table_metrics_runs
      WHERE snapshot_date = DATE '2025-10-02' AND status='SUCCEEDED'
      ORDER BY collected_at DESC
      LIMIT 1
    ),
    agg AS (
      SELECT jsonb_object_agg(schema_name, tables) AS payload
      FROM (
        SELECT schema_name, jsonb_object_agg(table_name, row_count) AS tables
        FROM nms.table_metrics_measurements
        WHERE run_id = (SELECT run_id FROM last_run)
        GROUP BY schema_name
      ) s
    )
    INSERT INTO nms.table_metrics_daily_snapshot (snapshot_date, run_id, payload)
    SELECT DATE '2025-10-02',
           (SELECT run_id FROM last_run),
           (SELECT payload FROM agg)
    ON CONFLICT (snapshot_date) DO UPDATE
      SET run_id = EXCLUDED.run_id,
          payload = EXCLUDED.payload,
          collected_at = now()
    WHERE nms.table_metrics_daily_snapshot.run_id IS NULL
       OR EXCLUDED.run_id > nms.table_metrics_daily_snapshot.run_id;
    ```

---

## 13) FAQ

**Q: Why both run and daily snapshots?**
A: Run snapshots preserve **every** execution (audit, forensics). Daily gives downstreams a single, stable record per day.

**Q: What if the function runs multiple times in one day?**
A: You’ll get multiple **run** snapshots; the **daily** snapshot overwrites to the newest `run_id`.

**Q: Can we store only the daily snapshot to save space?**
A: Yes—drop the run snapshot write. But you’ll lose per-run history.

**Q: Can the frontend call the DB directly?**
A: For internal dashboards, yes. Otherwise, consider a tiny HTTP reader service.

**Q: Does this require SQLAlchemy ORM?**
A: No. We use **SQLAlchemy Core** with `table()/column()` and PostgreSQL dialect upserts.

---

## 14) Glossary

* **Run**: one execution of the function (one header row).
* **Measurement**: a table’s row count captured during a run.
* **Snapshot (run)**: JSON representation of all measurements for a single run.
* **Snapshot (daily)**: JSON representation for the last successful run on a given business day.

---

## 15) Quick Reference (request, outputs)

**Request JSON (Pub/Sub message):**

```json
{}
```

or

```json
{
  "skipSchemas": ["tmp", "stage"],
  "exclude": ["public.orders", "sales.customers"]
}
```

**Snapshot JSON payload shape:**

```json
{
  "public": {
    "orders": 1215,
    "order_items": 3542
  },
  "sales": {
    "customers": 451
  }
}
```

---

If you want this turned into a Markdown page with diagrams or a README-style doc for your repo, I can format it that way verbatim.
