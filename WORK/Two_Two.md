Here’s a clean, deployment-free explainer you can drop into your docs. It shows **what each of the 4 tables is for**, how they relate, and how they’re used—**in a tabular way** plus a few crisp examples.

---

# Model at a glance

| Table                              | Grain (one row = …)                | Primary Purpose                                            | Key Columns                                                                                                 | Typical Writes                                                | Typical Reads                                              |
| ---------------------------------- | ---------------------------------- | ---------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------- | ---------------------------------------------------------- |
| `nms.table_metrics_runs`           | **One function execution** (“run”) | Audit & control of each run (status, time, filters used)   | `run_id` (PK), `collected_at`, `snapshot_date`, `status`, `error_message`, `skip_schemas`, `exclude_tables` | Insert `RUNNING` at start → update to `SUCCEEDED` or `FAILED` | Latest run, runs by day/status, link to detail & snapshots |
| `nms.table_metrics_measurements`   | **One table measured in one run**  | Normalized facts (row counts per table per run)            | `(run_id, schema_name, table_name)` (PK), `row_count`                                                       | Bulk insert N rows (one per target table)                     | Trends (per table over days), comparisons between runs     |
| `nms.table_metrics_run_snapshot`   | **One snapshot per run**           | Quick **point-in-time JSON** for that run                  | `run_id` (PK), `payload` (JSON)                                                                             | Upsert once per run                                           | “Give me the entire result of run X” or “latest run”       |
| `nms.table_metrics_daily_snapshot` | **One snapshot per business day**  | Stable **daily JSON** (latest successful run for that day) | `snapshot_date` (PK), `run_id`, `payload` (JSON)                                                            | Upsert **only if** incoming `run_id` is newer                 | “Give me today’s numbers” / “Give me 2025-10-02”           |

---

## How they work together (high level)

| Step | Action                                          | Table(s) touched               | Why it matters                                                                                    |
| ---- | ----------------------------------------------- | ------------------------------ | ------------------------------------------------------------------------------------------------- |
| 1    | Create run header (`RUNNING`)                   | `table_metrics_runs`           | Reserves a `run_id` and gives you auditability even if later steps fail.                          |
| 2    | Discover targets & `COUNT(*)`                   | —                              | Pure read from `information_schema` and source tables.                                            |
| 3    | Write per-table facts                           | `table_metrics_measurements`   | Source-of-truth facts: one row per (run, schema, table).                                          |
| 4    | Build JSON payload `{schema: {table: count}}`   | —                              | Materialization used for snapshots.                                                               |
| 5    | Upsert **run snapshot**                         | `table_metrics_run_snapshot`   | Append-only history; forensic/“show me that exact run”.                                           |
| 6    | Upsert **daily snapshot** (newer `run_id` wins) | `table_metrics_daily_snapshot` | Stable “one row per day” for easy reads; auto-overwrites if a later run on the same day succeeds. |
| 7    | Mark header `SUCCEEDED` / `FAILED`              | `table_metrics_runs`           | Closes the loop; downstreams can trust `status`.                                                  |

---

## What each table buys you (in practice)

| Table                          | You’d use it when you want to…                                      | Pros                                                     | Trade-offs                                               |
| ------------------------------ | ------------------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- |
| `table_metrics_runs`           | list runs, see failures, know when/with what filters a run executed | Simple audit trail; supports ops & alerting              | Doesn’t store the counts—just the run metadata           |
| `table_metrics_measurements`   | chart trends per table, compare runs, compute deltas                | Fully normalized; flexible analytics                     | Slightly more joins for UI if you need the whole picture |
| `table_metrics_run_snapshot`   | fetch the **exact** output of a specific run, or the latest run     | Single read returns the whole `{schema: {table: count}}` | Grows with every run (by design); not “one per day”      |
| `table_metrics_daily_snapshot` | power dashboards with **one** JSON per day                          | Stable read model; hides “how many runs happened”        | Overwrites within a day (last-successful-run wins)       |

---

## Typical queries (copy/paste)

**Latest run (metadata)**

```sql
SELECT run_id, collected_at, status
FROM nms.table_metrics_runs
ORDER BY collected_at DESC
LIMIT 1;
```

**Today’s daily JSON**

```sql
SELECT payload
FROM nms.table_metrics_daily_snapshot
WHERE snapshot_date = CURRENT_DATE;
```

**Latest run’s JSON (point-in-time)**

```sql
SELECT j.payload
FROM nms.table_metrics_run_snapshot j
JOIN nms.table_metrics_runs r USING (run_id)
ORDER BY r.collected_at DESC
LIMIT 1;
```

**7-day trend for a table**

```sql
SELECT r.snapshot_date, d.row_count
FROM nms.table_metrics_runs r
JOIN nms.table_metrics_measurements d USING (run_id)
WHERE d.schema_name = 'public'
  AND d.table_name  = 'orders'
  AND r.snapshot_date >= CURRENT_DATE - 7
ORDER BY r.snapshot_date;
```

**Rebuild a daily snapshot from detail (if needed)**

```sql
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
INSERT INTO nms.table_metrics_daily_snapshot (snapshot_date, run_id, payload, collected_at)
SELECT DATE '2025-10-02',
       (SELECT run_id FROM last_run),
       (SELECT payload FROM agg),
       now()
ON CONFLICT (snapshot_date) DO UPDATE
  SET run_id = EXCLUDED.run_id,
      payload = EXCLUDED.payload,
      collected_at = now()
WHERE nms.table_metrics_daily_snapshot.run_id IS NULL
   OR EXCLUDED.run_id > nms.table_metrics_daily_snapshot.run_id;
```

---

## Example rows (human-readable)

**`table_metrics_runs`**

```
run_id | collected_at           | snapshot_date | status     | skip_schemas     | exclude_tables
------+-------------------------+---------------+------------+------------------+----------------
 501  | 2025-10-02 05:15:02+00  | 2025-10-02    | SUCCEEDED  | ["tmp","stage"]  | []
 502  | 2025-10-02 06:47:31+00  | 2025-10-02    | SUCCEEDED  | ["tmp","stage"]  | []
```

**`table_metrics_measurements` (subset)**

```
run_id | schema_name |  table_name  | row_count
------+-------------+--------------+-----------
 501  | public      | orders       | 1200
 501  | public      | order_items  | 3500
 502  | public      | orders       | 1215
 502  | public      | order_items  | 3542
```

**`table_metrics_run_snapshot` (per run)**

```
run_id | payload
------+---------------------------------------------------------
 501  | {"public":{"orders":1200,"order_items":3500}}
 502  | {"public":{"orders":1215,"order_items":3542}}
```

**`table_metrics_daily_snapshot` (one row/day)**

```
snapshot_date | run_id | payload
-------------+--------+-----------------------------------------
2025-10-02   | 502    | {"public":{"orders":1215,"order_items":3542}}
```

---

## Why this normalized + snapshot combo?

* **Normalized detail** (`runs`, `measurements`) is your **canonical source** for analytics and audit. It scales, it’s easy to extend (add metrics), and you can always recompute read models.
* **Snapshots** (`run_snapshot`, `daily_snapshot`) are **read-optimized views**: one call → one JSON. They keep UI/simple jobs fast and boring, while the normalized layer keeps you future-proof.

If you want, I can add a final “Glossary” box or an ASCII relationship diagram for your README.
