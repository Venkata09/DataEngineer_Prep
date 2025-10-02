Absolutely — here’s a **complete, consistent package**: the **full DDL (schema)** and the **final Python (SQLAlchemy Core)** that writes to those tables using the PostgreSQL dialect (proper upserts). This version uses `sa.Table` + `MetaData(schema="nms")` (robust and clear), and it matches the normalization + snapshots design we aligned on.

---

# 1) Database schema (AlloyDB/Postgres)

> Uses a **regular column** for `snapshot_date` with a timezone-fixed default (no generated column), avoiding the “immutable expression” error you hit earlier.

```sql
-- Create schema
CREATE SCHEMA IF NOT EXISTS nms;

-- 1) Runs (audit/control)
CREATE TABLE IF NOT EXISTS nms.table_metrics_runs (
  run_id         BIGSERIAL    PRIMARY KEY,
  collected_at   timestamptz  NOT NULL DEFAULT now(),
  -- Business-day date; fix TZ to your business timezone:
  snapshot_date  date         NOT NULL DEFAULT ((now() AT TIME ZONE 'America/New_York')::date),
  status         text         NOT NULL CHECK (status IN ('RUNNING','SUCCEEDED','FAILED')),
  error_message  text,
  skip_schemas   jsonb        NOT NULL DEFAULT '[]'::jsonb,
  exclude_tables jsonb        NOT NULL DEFAULT '[]'::jsonb
);
CREATE INDEX IF NOT EXISTS tm_runs_date_idx
  ON nms.table_metrics_runs (snapshot_date);

-- 2) Per-table measurements (normalized facts)
CREATE TABLE IF NOT EXISTS nms.table_metrics_measurements (
  run_id      bigint NOT NULL REFERENCES nms.table_metrics_runs(run_id) ON DELETE CASCADE,
  schema_name text   NOT NULL,
  table_name  text   NOT NULL,
  row_count   bigint NOT NULL,
  PRIMARY KEY (run_id, schema_name, table_name)
);
CREATE INDEX IF NOT EXISTS tm_meas_tbl_idx
  ON nms.table_metrics_measurements (schema_name, table_name, run_id);

-- 3A) Snapshot per run (append-only; point-in-time)
CREATE TABLE IF NOT EXISTS nms.table_metrics_run_snapshot (
  run_id       bigint      PRIMARY KEY REFERENCES nms.table_metrics_runs(run_id) ON DELETE CASCADE,
  payload      jsonb       NOT NULL,   -- {"schema":{"table":count,...},...}
  collected_at timestamptz NOT NULL DEFAULT now()
);

-- 3B) Snapshot per business day (one row/day; last-successful-run wins)
CREATE TABLE IF NOT EXISTS nms.table_metrics_daily_snapshot (
  snapshot_date date        PRIMARY KEY,
  run_id        bigint      REFERENCES nms.table_metrics_runs(run_id),
  payload       jsonb       NOT NULL,
  collected_at  timestamptz NOT NULL DEFAULT now()
);
```

> If your business timezone isn’t Eastern, change `'America/New_York'` in the DDL and also set `SNAPSHOT_TZ` in the function.

---

# 2) Cloud Function (CloudEvent) — **SQLAlchemy Core**, PostgreSQL upserts

**Key points**

* Uses your **ConnectionManager** (no psycopg2 DSN) via `creator=` on the engine.
* SQLAlchemy **Core** (`sa.Table` + **PostgreSQL upserts** via `sqlalchemy.dialects.postgresql.insert`).
* Request contract (inverse):
  `{"skipSchemas": ["tmp","stage"], "exclude": ["public.orders", ...]}` — all optional.

```python
# main.py
import os
import json
import base64
import logging
import datetime
from typing import Dict, List, Tuple, Optional
from zoneinfo import ZoneInfo

import sqlalchemy as sa
from sqlalchemy.pool import NullPool
from sqlalchemy.dialects.postgresql import insert as pg_insert, JSONB
from cloudevents.http import CloudEvent

# Your project's DB connection manager (should return a DB-API connection)
from .connections import ConnectionManager  # type: ignore

# ── Config ───────────────────────────────────────────────────────────────────
SYSTEM_SCHEMAS = {"pg_catalog", "information_schema", "pg_toast"}
SNAPSHOT_TZ = os.environ.get("SNAPSHOT_TZ", "America/New_York")

# ── Engine (uses your ConnectionManager; no DSN/pooling) ─────────────────────
def _dbapi_creator():
    cm = ConnectionManager()
    if hasattr(cm, "get_connection"):
        return cm.get_connection()
    if hasattr(cm, "connect"):
        return cm.connect()
    return cm()  # if the manager is callable

# pg8000 works well with AlloyDB connector; add `pg8000` to requirements.txt
engine = sa.create_engine(
    "postgresql+pg8000://",
    creator=_dbapi_creator,
    poolclass=NullPool,
    future=True,
)

# ── Table metadata (no reflection; columns match the DDL) ────────────────────
meta = sa.MetaData(schema="nms")

runs = sa.Table(
    "table_metrics_runs", meta,
    sa.Column("run_id", sa.BigInteger, primary_key=True),
    sa.Column("collected_at", sa.TIMESTAMP(timezone=True), nullable=False),
    sa.Column("snapshot_date", sa.Date, nullable=False),
    sa.Column("status", sa.Text, nullable=False),
    sa.Column("error_message", sa.Text),
    sa.Column("skip_schemas", JSONB, nullable=False),
    sa.Column("exclude_tables", JSONB, nullable=False),
)

meas = sa.Table(
    "table_metrics_measurements", meta,
    sa.Column("run_id", sa.BigInteger, nullable=False),
    sa.Column("schema_name", sa.Text, nullable=False),
    sa.Column("table_name", sa.Text, nullable=False),
    sa.Column("row_count", sa.BigInteger, nullable=False),
    sa.PrimaryKeyConstraint("run_id", "schema_name", "table_name"),
)

run_snap = sa.Table(
    "table_metrics_run_snapshot", meta,
    sa.Column("run_id", sa.BigInteger, primary_key=True),
    sa.Column("payload", JSONB, nullable=False),
    sa.Column("collected_at", sa.TIMESTAMP(timezone=True)),
)

day_snap = sa.Table(
    "table_metrics_daily_snapshot", meta,
    sa.Column("snapshot_date", sa.Date, primary_key=True),
    sa.Column("run_id", sa.BigInteger),
    sa.Column("payload", JSONB, nullable=False),
    sa.Column("collected_at", sa.TIMESTAMP(timezone=True)),
)

# ── Helpers ───────────────────────────────────────────────────────────────────
def _quote_ident(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'

def _parse_event_body(cloud_event: CloudEvent) -> dict:
    data = cloud_event.data
    # Pub/Sub envelope (Eventarc)
    if isinstance(data, dict) and "message" in data and isinstance(data["message"], dict):
        b64 = data["message"].get("data", "") or ""
        if b64:
            return json.loads(base64.b64decode(b64).decode("utf-8") or "{}")
        return {}
    # Direct dict / JSON
    if isinstance(data, dict):
        return data
    if isinstance(data, (bytes, str)):
        s = data.decode("utf-8") if isinstance(data, bytes) else data
        try:
            return json.loads(s)
        except Exception:
            return {}
    return {}

def _validate_request(body: dict) -> Tuple[List[str], List[str]]:
    """
    Expected JSON (all optional):
      { "exclude": ["schema.table", ...], "skipSchemas": ["tmp","stage"] }
    """
    exclude_tables: List[str] = []
    for fq in body.get("exclude", []) or []:
        if not isinstance(fq, str) or "." not in fq:
            raise ValueError(f"exclude items must be 'schema.table': {fq}")
        exclude_tables.append(fq)
    skip_schemas: List[str] = list(body.get("skipSchemas", []) or [])
    return exclude_tables, skip_schemas

def _list_all_tables(conn: sa.Connection, exclude_schemas: set) -> List[Tuple[str, str]]:
    res = conn.execute(sa.text("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
          AND table_schema NOT IN ('pg_catalog','information_schema','pg_toast')
    """))
    return [(s, t) for (s, t) in res.fetchall() if s not in exclude_schemas]

def _count_exact(conn: sa.Connection, schema: str, table: str) -> int:
    fq = f"{_quote_ident(schema)}.{_quote_ident(table)}"
    res = conn.execute(sa.text(f"SELECT COUNT(*) FROM {fq}"))
    return int(res.scalar_one())

# ── Core write ops (SQLAlchemy Core + Postgres upserts) ───────────────────────
def _insert_run_header(conn: sa.Connection, status: str,
                       skip_schemas: List[str], exclude_tables: List[str]) -> int:
    stmt = sa.insert(runs).values(
        status=status,
        skip_schemas=skip_schemas,
        exclude_tables=exclude_tables,
        # collected_at/snapshot_date default in DB; we let DB fill them
    ).returning(runs.c.run_id)
    return int(conn.execute(stmt).scalar_one())

def _update_run_status(conn: sa.Connection, run_id: int, status: str, error_message: Optional[str]):
    stmt = sa.update(runs).where(runs.c.run_id == run_id).values(
        status=status,
        error_message=error_message
    )
    conn.execute(stmt)

def _insert_measurements(conn: sa.Connection, rows: List[Tuple[int, str, str, int]]) -> None:
    """
    rows: list[(run_id, schema_name, table_name, row_count)]
    """
    rows_to_insert = [
        {"run_id": rid, "schema_name": s, "table_name": t, "row_count": cnt}
        for (rid, s, t, cnt) in rows
    ]
    if rows_to_insert:
        conn.execute(sa.insert(meas), rows_to_insert)

def _write_run_snapshot(conn: sa.Connection, run_id: int, payload_nested: Dict[str, Dict[str, int]]) -> None:
    stmt = pg_insert(run_snap).values(run_id=run_id, payload=payload_nested)
    stmt = stmt.on_conflict_do_update(
        index_elements=[run_snap.c.run_id],
        set_={"payload": stmt.excluded.payload, "collected_at": sa.func.now()},
    )
    conn.execute(stmt)

def _write_daily_snapshot(conn: sa.Connection, run_id: int, snapshot_date: datetime.date,
                          payload_nested: Dict[str, Dict[str, int]]) -> None:
    stmt = pg_insert(day_snap).values(
        snapshot_date=snapshot_date,
        run_id=run_id,
        payload=payload_nested,
    )
    # last-successful-run wins: only overwrite if incoming run_id is newer
    stmt = stmt.on_conflict_do_update(
        index_elements=[day_snap.c.snapshot_date],
        set_={
            "run_id": stmt.excluded.run_id,
            "payload": stmt.excluded.payload,
            "collected_at": sa.func.now(),
        },
        where=sa.or_(day_snap.c.run_id.is_(None), stmt.excluded.run_id > day_snap.c.run_id),
    )
    conn.execute(stmt)

# ── CloudEvent entrypoint ─────────────────────────────────────────────────────
def main(cloud_event: CloudEvent):
    """
    CloudEvent JSON payload (inverse):
      {"exclude":["schema.table", ...], "skipSchemas":["tmp","stage"]}
    """
    body = _parse_event_body(cloud_event)
    business_date = datetime.datetime.now(ZoneInfo(SNAPSHOT_TZ)).date()
    run_id: Optional[int] = None

    try:
        exclude_tables, skip_schemas = _validate_request(body)
        exclude_pairs = {tuple(fq.split(".", 1)) for fq in exclude_tables}

        # 1) Create RUNNING header (own txn so we have audit even if later fails)
        with engine.begin() as conn:
            run_id = _insert_run_header(conn, status="RUNNING",
                                        skip_schemas=skip_schemas,
                                        exclude_tables=exclude_tables)

        # 2) Do the work and mark success
        with engine.begin() as conn:
            # Discover & filter
            all_targets = _list_all_tables(conn, set(SYSTEM_SCHEMAS) | set(skip_schemas))
            targets = [(s, t) for (s, t) in all_targets if (s, t) not in exclude_pairs]
            if not targets:
                raise ValueError("no tables to process after applying exclusions")

            # Count → measurements + nested JSON
            measurements: List[Tuple[int, str, str, int]] = []
            payload: Dict[str, Dict[str, int]] = {}
            for (schema, table) in targets:
                cnt = _count_exact(conn, schema, table)
                measurements.append((run_id, schema, table, cnt))
                payload.setdefault(schema, {})[table] = cnt

            # Persist detail + snapshots
            _insert_measurements(conn, measurements)
            _write_run_snapshot(conn, run_id, payload)
            _write_daily_snapshot(conn, run_id, business_date, payload)

            # Mark SUCCEEDED
            _update_run_status(conn, run_id, "SUCCEEDED", None)

        return {
            "status": "ok",
            "runId": run_id,
            "businessDate": str(business_date),
            "tablesProcessed": sum(len(v) for v in payload.values()),
        }

    except Exception as e:
        logging.exception("table metrics job failed")
        # Try to mark FAILED if we created a header; else create a FAILED header for audit
        try:
            if run_id is not None:
                with engine.begin() as conn:
                    _update_run_status(conn, run_id, "FAILED", str(e)[:4000])
            else:
                with engine.begin() as conn:
                    _ = _insert_run_header(
                        conn, status="FAILED",
                        skip_schemas=body.get("skipSchemas", []) if isinstance(body, dict) else [],
                        exclude_tables=body.get("exclude", []) if isinstance(body, dict) else [],
                    )
        except Exception:
            pass
        return {"status": "error", "message": str(e)}
```

---

## 3) Sample request bodies (Pub/Sub → CloudEvent)

```json
{}
```

```json
{ "skipSchemas": ["tmp", "stage"] }
```

```json
{ "exclude": ["public.orders", "sales.customers"] }
```

```json
{
  "skipSchemas": ["tmp", "stage"],
  "exclude": ["public.orders_archive", "analytics.events_old"]
}
```

---

### Notes / sanity checks

* The **DDL and Python column names match exactly**.
* `snapshot_date` is now a **regular column** with a default based on a fixed timezone; this avoids the “immutable” error from generated columns.
* The function still uses `SNAPSHOT_TZ` only for the **response**/log date (`business_date`). Ideally keep DB default TZ and `SNAPSHOT_TZ` aligned.
* Engine uses `creator=` to source a DB-API connection from your **ConnectionManager** (works with the AlloyDB connector). Add `pg8000` to dependencies if your connector returns a pg8000 connection.

If you want me to tweak table names to the short `ts_*` scheme or add additional metrics (bytes, etc.), say the word and I’ll adjust both DDL + code.
