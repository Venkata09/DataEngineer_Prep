Perfect—let’s switch to **SQLAlchemy** (no psycopg) and keep it super direct:

* Read ZIP from GCS
* Extract the `.sql` files
* Order them so `create_schema_ads*.sql` runs **first**, then the rest **alphabetically**
* Execute each file in its **own transaction** using SQLAlchemy + **pg8000** (pure-Python Postgres driver)

---

# Minimal, drop-in service (Cloud Functions v2 or Cloud Run)

## requirements.txt

```
google-cloud-storage==2.*
SQLAlchemy==2.*
pg8000==1.*
sqlparse==0.5.*
Flask==3.*   # only needed if you want the optional HTTP endpoint (Cloud Run)
```

## main.py

```python
import os, io, hashlib, zipfile, pathlib
from typing import List
from google.cloud import storage
import sqlparse

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# ------------------------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------------------------
# Use pg8000 DBAPI via SQLAlchemy URL:
#   postgresql+pg8000://user:pass@HOST:5432/DBNAME
DB_URL = os.environ.get("DB_URL")  # REQUIRED
DB_SEARCH_PATH = os.environ.get("DB_SEARCH_PATH", "public")

# ------------------------------------------------------------------------------
# ConnectionFactory (simple)
#   - Keeps a single Engine for reuse (Cloud Run/Functions warm instances)
#   - If you later need multiple targets, read DSNs from env/secrets by target_id
# ------------------------------------------------------------------------------
class ConnectionFactory:
    _engine: Engine | None = None

    @classmethod
    def get_engine(cls) -> Engine:
        if cls._engine is None:
            if not DB_URL:
                raise RuntimeError("DB_URL env var is not set")
            # Small, predictable pool (adjust if you raise service concurrency)
            cls._engine = create_engine(
                DB_URL,
                pool_pre_ping=True,
                pool_size=2,
                max_overflow=0,
                future=True,
            )
        return cls._engine

# ------------------------------------------------------------------------------
# Ordering rule:
#   1) any file starting with "create_schema_ads" (case-insensitive) comes first
#   2) the rest of .sql files in case-insensitive alphabetical order
# ------------------------------------------------------------------------------
def order_sql_files(names: List[str]) -> List[str]:
    sqls = [n for n in names if n.lower().endswith(".sql")]
    if not sqls:
        raise RuntimeError("No .sql files found in ZIP")

    first = [n for n in sqls if pathlib.Path(n).name.lower().startswith("create_schema_ads")]
    rest  = sorted([n for n in sqls if n not in first], key=lambda s: pathlib.Path(s).name.lower())

    if not first:
        raise RuntimeError("Missing required 'create_schema_ads*.sql'")
    if len(first) > 1:
        raise RuntimeError("Multiple 'create_schema_ads*.sql' files found; keep exactly one.")
    return first + rest

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

# ------------------------------------------------------------------------------
# SQL execution helpers
#   - Some drivers don’t allow multiple statements per execute().
#   - We try full-file first; if the driver errors on multi-statement, we split.
#   - sqlparse.split() avoids naive semicolon bugs, incl. function bodies.
# ------------------------------------------------------------------------------
def _exec_file_sql(engine: Engine, file_name: str, sql_text: str):
    with engine.begin() as conn:
        # session defaults per-file
        conn.exec_driver_sql(f"SET search_path TO {DB_SEARCH_PATH}")
        conn.exec_driver_sql("SET lock_timeout = '30s'")
        conn.exec_driver_sql("SET statement_timeout = '10min'")

        try:
            # Try as one chunk (driver may accept multi-statement)
            conn.exec_driver_sql(sql_text)
        except Exception as multi_stmt_err:
            # Fallback: split into discrete statements and run sequentially
            stmts = [s.strip() for s in sqlparse.split(sql_text) if s.strip()]
            if len(stmts) <= 1:
                # If it's still one stmt, re-raise original error
                raise

            for s in stmts:
                conn.exec_driver_sql(s)

# ------------------------------------------------------------------------------
# Idempotency ledger:
#   - Records each file applied with checksum.
#   - Re-run safely skips if same file+checksum was already applied.
#   - If same file name but different checksum appears, fail fast (drift).
# ------------------------------------------------------------------------------
DDL_CREATE_LEDGER = """
CREATE TABLE IF NOT EXISTS ops_migrations (
  id BIGSERIAL PRIMARY KEY,
  file_name TEXT NOT NULL,
  file_checksum TEXT NOT NULL,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  status TEXT NOT NULL CHECK (status IN ('applied','failed')),
  error TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_ops_migrations_file ON ops_migrations(file_name);
"""

def ensure_ledger(engine: Engine):
    with engine.begin() as conn:
        conn.exec_driver_sql(DDL_CREATE_LEDGER)

def already_applied(engine: Engine, file_name: str, checksum: str) -> bool:
    with engine.begin() as conn:
        row = conn.execute(
            text("""SELECT file_checksum, status FROM ops_migrations WHERE file_name=:f"""),
            {"f": file_name},
        ).first()
        if not row:
            return False
        # If checksum is different, flag drift
        if row[0] != checksum:
            raise RuntimeError(
                f"Checksum mismatch for {file_name} (existing={row[0]}, new={checksum}). Abort."
            )
        return row[1] == "applied"

def mark_result(engine: Engine, file_name: str, checksum: str, status: str, error: str | None = None):
    with engine.begin() as conn:
        if status == "applied":
            conn.execute(
                text("""INSERT INTO ops_migrations(file_name, file_checksum, status) 
                        VALUES (:f, :c, 'applied')
                        ON CONFLICT (file_name) DO UPDATE
                          SET file_checksum=EXCLUDED.file_checksum, status='applied', error=NULL"""),
                {"f": file_name, "c": checksum},
            )
        else:
            conn.execute(
                text("""INSERT INTO ops_migrations(file_name, file_checksum, status, error) 
                        VALUES (:f, :c, 'failed', :e)
                        ON CONFLICT (file_name) DO UPDATE
                          SET file_checksum=EXCLUDED.file_checksum, status='failed', error=:e"""),
                {"f": file_name, "c": checksum, "e": (error or "")[:2000]},
            )

# ------------------------------------------------------------------------------
# Core: download ZIP from GCS, order files, execute in order
# ------------------------------------------------------------------------------
def run_plan_from_gcs(bucket: str, name: str):
    storage_client = storage.Client()
    blob = storage_client.bucket(bucket).blob(name)
    data = blob.download_as_bytes()

    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        members = zf.namelist()
        ordered = order_sql_files(members)
        files = []
        for m in ordered:
            content = zf.read(m)
            files.append((pathlib.Path(m).name, content, sha256_bytes(content)))

    eng = ConnectionFactory.get_engine()
    ensure_ledger(eng)

    # Apply each file once, in order
    for fname, content_bytes, checksum in files:
        if already_applied(eng, fname, checksum):
            print(f"[skip] {fname} (already applied)")
            continue

        sql_text = content_bytes.decode("utf-8")
        print(f"[apply] {fname}")
        try:
            _exec_file_sql(eng, fname, sql_text)
            mark_result(eng, fname, checksum, "applied")
        except Exception as e:
            mark_result(eng, fname, checksum, "failed", str(e))
            raise

    print("[done] all files applied (or skipped)")

# ------------------------------------------------------------------------------
# Entrypoints
#   - Cloud Functions (2nd gen) CloudEvent (Eventarc Storage 'finalized')
#   - Optional HTTP for Cloud Run/local testing: POST /run {"bucket": "...", "name": "..."}
# ------------------------------------------------------------------------------
def gcs_trigger(cloud_event):
    data = cloud_event.data  # {"bucket": "...", "name": "...", ...}
    run_plan_from_gcs(data["bucket"], data["name"])
    return "ok"

# Optional HTTP app (Cloud Run/testing)
from flask import Flask, request
app = Flask(__name__)

@app.post("/run")
def http_run():
    body = request.get_json(force=True)
    run_plan_from_gcs(body["bucket"], body["name"])
    return {"status": "ok"}
```

---

## How this meets your needs

* **No psycopg**: we use **SQLAlchemy 2.x** with **pg8000** as the Postgres DBAPI.
* **Ordering**: `create_schema_ads*.sql` first → others alphabetically (case-insensitive).
* **Per-file transaction**: each `.sql` runs in its own `BEGIN/COMMIT` scope.
* **Idempotent**: stores `(file_name, checksum)`—replays won’t double-apply; checksum mismatch fails fast (prevents “same name, different content” drift).
* **Zip anywhere**: files can be at root or nested—the code uses names from the ZIP and only the **basename** for ledger records.

---

## Quick deploy notes

* **Cloud Functions v2** (Python 3.11):

    * Handler: `gcs_trigger`
    * Trigger: Eventarc → Storage object **finalized** for your bucket/prefix
* **Env vars**:

    * `DB_URL=postgresql+pg8000://USER:PASS@HOST:5432/DB`
    * `DB_SEARCH_PATH=public` (or your schema)

> If later you want **multiple instances**, extend `ConnectionFactory` to map `target_id → DB_URL` (via env JSON or Secret Manager) and put `target_id` into the event payload.

If you want this in **C#/.NET** (SqlClient/Npgsql) or **Node.js** (knex/pg) with the same ordering and ledger behavior, I can drop that version as well.





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

# Your project's DB connection manager (returns a DB-API connection)
from .connections import ConnectionManager  # type: ignore

# ── Config ───────────────────────────────────────────────────────────────────
SYSTEM_SCHEMAS = {"pg_catalog", "information_schema", "pg_toast"}
SNAPSHOT_TZ = os.environ.get("SNAPSHOT_TZ", "America/New_York")

# ── Engine using your ConnectionManager (no DSN, no pool) ────────────────────
def _dbapi_creator():
    cm = ConnectionManager()
    if hasattr(cm, "get_connection"):
        return cm.get_connection()
    if hasattr(cm, "connect"):
        return cm.connect()
    return cm()  # if callable

engine = sa.create_engine(
    "postgresql+pg8000://",   # use pg8000; add it to requirements.txt
    creator=_dbapi_creator,
    poolclass=NullPool,
    future=True,
)

# ── Lightweight table/column definitions (no MetaData/reflect) ───────────────
runs = sa.table(
    "table_metrics_runs",
    sa.column("run_id", sa.BigInteger, primary_key=True),
    sa.column("collected_at", sa.TIMESTAMP(timezone=True)),
    sa.column("snapshot_date", sa.Date),
    sa.column("status", sa.Text),
    sa.column("error_message", sa.Text),
    sa.column("skip_schemas", JSONB),
    sa.column("exclude_tables", JSONB),
    schema="nms",
)

meas = sa.table(
    "table_metrics_measurements",
    sa.column("run_id", sa.BigInteger),
    sa.column("schema_name", sa.Text),
    sa.column("table_name", sa.Text),
    sa.column("row_count", sa.BigInteger),
    schema="nms",
)

run_snap = sa.table(
    "table_metrics_run_snapshot",
    sa.column("run_id", sa.BigInteger, primary_key=True),
    sa.column("payload", JSONB),
    sa.column("collected_at", sa.TIMESTAMP(timezone=True)),
    schema="nms",
)

day_snap = sa.table(
    "table_metrics_daily_snapshot",
    sa.column("snapshot_date", sa.Date, primary_key=True),
    sa.column("run_id", sa.BigInteger),
    sa.column("payload", JSONB),
    sa.column("collected_at", sa.TIMESTAMP(timezone=True)),
    schema="nms",
)

# ── Helpers ───────────────────────────────────────────────────────────────────
def _quote_ident(ident: str) -> str:
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
    fq = f"{_quote_ident(schema)}.{_quote_ident(table)}"
    res = conn.execute(sa.text(f"SELECT COUNT(*) FROM {fq}"))
    return int(res.scalar_one())

# ── Core write ops (all SQLAlchemy Core; Postgres upserts where needed) ───────
def _insert_run_header(conn: sa.Connection, status: str,
                       skip_schemas: List[str], exclude_tables: List[str]) -> int:
    stmt = sa.insert(runs).values(
        status=status,
        skip_schemas=skip_schemas,
        exclude_tables=exclude_tables,
    ).returning(runs.c.run_id)
    return int(conn.execute(stmt).scalar_one())

def _update_run_status(conn: sa.Connection, run_id: int, status: str, error_message: Optional[str]):
    stmt = sa.update(runs).where(runs.c.run_id == run_id).values(
        status=status, error_message=error_message
    )
    conn.execute(stmt)

def _insert_measurements(conn: sa.Connection, rows: List[Tuple[int, str, str, int]]) -> None:
    payload = [{"run_id": rid, "schema_name": s, "table_name": t, "row_count": cnt}
               for (rid, s, t, cnt) in rows]
    if payload:
        conn.execute(sa.insert(meas), payload)

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

        # 1) create a RUNNING header (own tx so it persists even if later work fails)
        with engine.begin() as conn:
            run_id = _insert_run_header(conn, status="RUNNING",
                                        skip_schemas=skip_schemas,
                                        exclude_tables=exclude_tables)

        # 2) do the work
        with engine.begin() as conn:
            all_targets = _list_all_tables(conn, set(SYSTEM_SCHEMAS) | set(skip_schemas))
            targets = [(s, t) for (s, t) in all_targets if (s, t) not in exclude_pairs]
            if not targets:
                raise ValueError("no tables to process after applying exclusions")

            # count → measurements + nested JSON
            measurements: List[Tuple[int, str, str, int]] = []
            payload: Dict[str, Dict[str, int]] = {}
            for (schema, table) in targets:
                cnt = _count_exact(conn, schema, table)
                measurements.append((run_id, schema, table, cnt))
                payload.setdefault(schema, {})[table] = cnt

            # write detail + snapshots; mark success
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
        logging.exception("table metrics job failed")
        # try to mark FAILED if header exists; otherwise create FAILED for audit
        try:
            if run_id is not None:
                with engine.begin() as conn:
                    _update_run_status(conn, run_id, "FAILED", str(e)[:4000])
            else:
                with engine.begin() as conn:
                    _ = _insert_run_header(conn, status="FAILED",
                                           skip_schemas=body.get("skipSchemas", []) if isinstance(body, dict) else [],
                                           exclude_tables=body.get("exclude", []) if isinstance(body, dict) else [])
        except Exception:
            pass
        return {"status": "error", "message": str(e)}




