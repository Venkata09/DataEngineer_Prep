Perfect—let’s (1) rename your tables to the `db_metrics_*` scheme and (2) modularize so names/schemas are **config-driven**. We’ll use **`sa.table()`/`sa.column()`** (lightweight, no reflection) and a tiny `config.py` + `tables.py`.

---

# `config.py` — central config (env-overridable)

```python
# config.py
import os

DEFAULT_SCHEMA = "nms"
DEFAULT_TABLES = {
    "runs": "db_metrics_runs",
    "metrics": "db_metrics",
    "run_snapshot": "db_metrics_run_snapshot",
    "daily_snapshot": "db_metrics_daily_snapshot",
}

def load_settings():
    """Read schema/table names and TZ from env (with safe defaults)."""
    schema = os.getenv("DB_SCHEMA", DEFAULT_SCHEMA)
    tables = {
        "runs":          os.getenv("TBL_DB_METRICS_RUNS",           DEFAULT_TABLES["runs"]),
        "metrics":       os.getenv("TBL_DB_METRICS",                DEFAULT_TABLES["metrics"]),
        "run_snapshot":  os.getenv("TBL_DB_METRICS_RUN_SNAPSHOT",   DEFAULT_TABLES["run_snapshot"]),
        "daily_snapshot":os.getenv("TBL_DB_METRICS_DAILY_SNAPSHOT", DEFAULT_TABLES["daily_snapshot"]),
    }
    snapshot_tz = os.getenv("SNAPSHOT_TZ", "America/New_York")
    return schema, tables, snapshot_tz
```

---

# `tables.py` — factory for lightweight SQLAlchemy table objects

```python
# tables.py
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from types import SimpleNamespace

def make_tables(schema: str, names: dict):
    runs = sa.table(
        names["runs"],
        sa.column("run_id", sa.BigInteger),
        sa.column("collected_at", sa.TIMESTAMP(timezone=True)),
        sa.column("snapshot_date", sa.Date),
        sa.column("status", sa.Text),
        sa.column("error_message", sa.Text),
        sa.column("skip_schemas", JSONB),
        sa.column("exclude_tables", JSONB),
        schema=schema,
    )

    metrics = sa.table(
        names["metrics"],
        sa.column("run_id", sa.BigInteger),
        sa.column("schema_name", sa.Text),
        sa.column("table_name", sa.Text),
        sa.column("row_count", sa.BigInteger),
        schema=schema,
    )

    run_snap = sa.table(
        names["run_snapshot"],
        sa.column("run_id", sa.BigInteger),
        sa.column("payload", JSONB),
        sa.column("collected_at", sa.TIMESTAMP(timezone=True)),
        schema=schema,
    )

    day_snap = sa.table(
        names["daily_snapshot"],
        sa.column("snapshot_date", sa.Date),
        sa.column("run_id", sa.BigInteger),
        sa.column("payload", JSONB),
        sa.column("collected_at", sa.TIMESTAMP(timezone=True)),
        schema=schema,
    )

    return SimpleNamespace(runs=runs, metrics=metrics, run_snap=run_snap, day_snap=day_snap)
```

---

# `main.py` — updated to use config + table factory (single-transaction)

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

from .connections import ConnectionManager  # your connector
from .config import load_settings
from .tables import make_tables

# ── Load config (schema/table names/TZ) ───────────────────────────────────────
SCHEMA, TBL_NAMES, SNAPSHOT_TZ = load_settings()
SYSTEM_SCHEMAS = {"pg_catalog", "information_schema", "pg_toast"}

# ── Build lightweight tables (sa.table/sa.column) ────────────────────────────
T = make_tables(SCHEMA, TBL_NAMES)  # T.runs, T.metrics, T.run_snap, T.day_snap

# ── Engine (uses your ConnectionManager; no pooling) ─────────────────────────
def _dbapi_creator():
    cm = ConnectionManager()
    if hasattr(cm, "get_connection"): return cm.get_connection()
    if hasattr(cm, "connect"):        return cm.connect()
    return cm()

engine = sa.create_engine(
    "postgresql+pg8000://",  # change to postgresql+psycopg:// if your manager returns psycopg
    creator=_dbapi_creator,
    poolclass=NullPool,
    future=True,
)

# ── Helpers ──────────────────────────────────────────────────────────────────
def _qi(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'

def _parse_event_body(cloud_event: CloudEvent) -> dict:
    data = cloud_event.data
    if isinstance(data, dict) and "message" in data and isinstance(data["message"], dict):
        b64 = data["message"].get("data", "") or ""
        if b64:
            return json.loads(base64.b64decode(b64).decode("utf-8") or "{}")
        return {}
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
    fq = f"{_qi(schema)}.{_qi(table)}"
    return int(conn.execute(sa.text(f"SELECT COUNT(*) FROM {fq}")).scalar_one())

# ── DML (single-transaction in main()) ───────────────────────────────────────
def _insert_run_header(conn: sa.Connection, status: str,
                       skip_schemas: List[str], exclude_tables: List[str]) -> int:
    stmt = sa.insert(T.runs).values(
        status=status,
        skip_schemas=skip_schemas,
        exclude_tables=exclude_tables,
    ).returning(T.runs.c.run_id)
    return int(conn.execute(stmt).scalar_one())

def _update_run_status(conn: sa.Connection, run_id: int, status: str, error_message: Optional[str]):
    conn.execute(
        sa.update(T.runs)
        .where(T.runs.c.run_id == run_id)
        .values(status=status, error_message=error_message)
    )

def _insert_measurements(conn: sa.Connection, rows: List[Tuple[int, str, str, int]]) -> None:
    rows_to_insert = [
        {"run_id": rid, "schema_name": s, "table_name": t, "row_count": cnt}
        for (rid, s, t, cnt) in rows
    ]
    if rows_to_insert:
        conn.execute(sa.insert(T.metrics).values(rows_to_insert))

def _write_run_snapshot(conn: sa.Connection, run_id: int, payload_nested: Dict[str, Dict[str, int]]) -> None:
    stmt = pg_insert(T.run_snap).values(run_id=run_id, payload=payload_nested)
    stmt = stmt.on_conflict_do_update(
        index_elements=[T.run_snap.c.run_id],
        set_={"payload": stmt.excluded.payload, "collected_at": sa.func.now()},
    )
    conn.execute(stmt)

def _write_daily_snapshot(conn: sa.Connection, run_id: int, snapshot_date: datetime.date,
                          payload_nested: Dict[str, Dict[str, int]]) -> None:
    stmt = pg_insert(T.day_snap).values(
        snapshot_date=snapshot_date,
        run_id=run_id,
        payload=payload_nested,
    ).on_conflict_do_update(
        index_elements=[T.day_snap.c.snapshot_date],
        set_={
            "run_id": sa.case(
                (T.day_snap.c.run_id.is_(None), sa.cast(sa.literal(run_id), sa.BigInteger)),
                else_=sa.func.greatest(T.day_snap.c.run_id, sa.cast(sa.literal(run_id), sa.BigInteger)),
            ),
            "payload": sa.case(
                (sa.or_(T.day_snap.c.run_id.is_(None), sa.literal(run_id) > T.day_snap.c.run_id),
                 sa.cast(sa.literal(payload_nested), JSONB)),
                else_=T.day_snap.c.payload,
            ),
            "collected_at": sa.func.now(),
        },
        where=sa.or_(T.day_snap.c.run_id.is_(None), sa.literal(run_id) > T.day_snap.c.run_id),
    )
    conn.execute(stmt)

# ── CloudEvent entrypoint (ONE TRANSACTION, atomic) ───────────────────────────
def main(cloud_event: CloudEvent):
    """
    Atomic run: header → counts → metrics → snapshots → mark SUCCEEDED.
    If ANY step fails, the transaction is rolled back and NOTHING is written.
    """
    body = _parse_event_body(cloud_event)
    business_date = datetime.datetime.now(ZoneInfo(SNAPSHOT_TZ)).date()

    try:
        exclude_tables, skip_schemas = _validate_request(body)
        exclude_pairs = {tuple(fq.split(".", 1)) for fq in exclude_tables}

        with engine.begin() as conn:  # commit on success, rollback on error
            run_id = _insert_run_header(conn, "RUNNING", skip_schemas, exclude_tables)

            all_targets = _list_all_tables(conn, set(SYSTEM_SCHEMAS) | set(skip_schemas))
            targets = [(s, t) for (s, t) in all_targets if (s, t) not in exclude_pairs]
            if not targets:
                raise ValueError("no tables to process after applying exclusions")

            measurements: List[Tuple[int, str, str, int]] = []
            payload: Dict[str, Dict[str, int]] = {}
            for (schema, table) in targets:
                cnt = _count_exact(conn, schema, table)
                measurements.append((run_id, schema, table, cnt))
                payload.setdefault(schema, {})[table] = cnt

            _insert_measurements(conn, measurements)
            _write_run_snapshot(conn, run_id, payload)
            _write_daily_snapshot(conn, run_id, business_date, payload)
            _update_run_status(conn, run_id, "SUCCEEDED", None)

        return {
            "status": "ok",
            "runId": run_id,
            "businessDate": str(business_date),
            "tablesProcessed": sum(len(v) for v in payload.values()),
        }

    except Exception as e:
        logging.exception("db metrics job failed (atomic rollback)")
        return {"status": "error", "message": str(e)}
```

---

## Updated DDL (new names)

```sql
CREATE SCHEMA IF NOT EXISTS nms;

-- 1) Runs
CREATE TABLE IF NOT EXISTS nms.db_metrics_runs (
  run_id         BIGSERIAL    PRIMARY KEY,
  collected_at   timestamptz  NOT NULL DEFAULT now(),
  snapshot_date  date         NOT NULL DEFAULT ((now() AT TIME ZONE 'America/New_York')::date),
  status         text         NOT NULL CHECK (status IN ('RUNNING','SUCCEEDED','FAILED')),
  error_message  text,
  skip_schemas   jsonb        NOT NULL DEFAULT '[]'::jsonb,
  exclude_tables jsonb        NOT NULL DEFAULT '[]'::jsonb
);
CREATE INDEX IF NOT EXISTS dbm_runs_date_idx ON nms.db_metrics_runs (snapshot_date);

-- 2) Per-table metrics
CREATE TABLE IF NOT EXISTS nms.db_metrics (
  run_id      bigint NOT NULL REFERENCES nms.db_metrics_runs(run_id) ON DELETE CASCADE,
  schema_name text   NOT NULL,
  table_name  text   NOT NULL,
  row_count   bigint NOT NULL,
  PRIMARY KEY (run_id, schema_name, table_name)
);
CREATE INDEX IF NOT EXISTS dbm_tbl_idx ON nms.db_metrics (schema_name, table_name, run_id);

-- 3A) Per-run snapshot
CREATE TABLE IF NOT EXISTS nms.db_metrics_run_snapshot (
  run_id       bigint      PRIMARY KEY REFERENCES nms.db_metrics_runs(run_id) ON DELETE CASCADE,
  payload      jsonb       NOT NULL,
  collected_at timestamptz NOT NULL DEFAULT now()
);

-- 3B) Per-day snapshot
CREATE TABLE IF NOT EXISTS nms.db_metrics_daily_snapshot (
  snapshot_date date        PRIMARY KEY,
  run_id        bigint      REFERENCES nms.db_metrics_runs(run_id),
  payload       jsonb       NOT NULL,
  collected_at  timestamptz NOT NULL DEFAULT now()
);
```

---

### Why this structure helps

* **No hardcoded names** in code; swap schema/table names via env vars:

  * `DB_SCHEMA`, `TBL_DB_METRICS_RUNS`, `TBL_DB_METRICS`, `TBL_DB_METRICS_RUN_SNAPSHOT`, `TBL_DB_METRICS_DAILY_SNAPSHOT`.
* **`sa.table()`** is lightweight, perfect for DML; DB holds constraints/indexes.
* Your **single transaction** guarantees commit on success, rollback on any failure.

Want me to also add a tiny `settings.example.env` you can copy to Cloud Functions env vars?
