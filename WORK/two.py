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


# ── Engine (uses your ConnectionManager) ──────────────────────────────────────
def _dbapi_creator():
    cm = ConnectionManager()
    # support either get_connection() or connect()
    if hasattr(cm, "get_connection"):
        return cm.get_connection()
    if hasattr(cm, "connect"):
        return cm.connect()
    return cm()  # if your manager is callable

# No pooling in Cloud Functions; one connection per use.
engine = sa.create_engine(
    "postgresql+pg8000://",  # compatible with AlloyDB connector returning pg8000 connection
    creator=_dbapi_creator,
    poolclass=NullPool,
    future=True,
)

# ── Table metadata (no create_all; DB already has the DDL) ────────────────────
meta = sa.MetaData(schema="nms")

runs = sa.Table(
    "table_metrics_runs", meta,
    sa.Column("run_id", sa.BigInteger, primary_key=True),
    sa.Column("collected_at", sa.TIMESTAMP(timezone=True)),   # DEFAULT now() in DB
    sa.Column("snapshot_date", sa.Date),                      # DEFAULT biz-date in DB
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

# ── Core write ops using PostgreSQL dialect upserts ───────────────────────────
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
        status=status,
        error_message=error_message
    )
    conn.execute(stmt)

def _insert_measurements(conn: sa.Connection, rows: List[Tuple[int, str, str, int]]) -> None:
    payload = [
        {"run_id": rid, "schema_name": s, "table_name": t, "row_count": cnt}
        for (rid, s, t, cnt) in rows
    ]
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
    CloudEvent payload (inverse):
      {"exclude":["schema.table", ...], "skipSchemas":["tmp","stage"]}
    """
    body = _parse_event_body(cloud_event)
    business_date = datetime.datetime.now(ZoneInfo(SNAPSHOT_TZ)).date()
    run_id: Optional[int] = None

    try:
        exclude_tables, skip_schemas = _validate_request(body)
        exclude_pairs = {tuple(fq.split(".", 1)) for fq in exclude_tables}

        # 1) create a RUNNING header in its own transaction (so it persists even if later work fails)
        with engine.begin() as conn:
            run_id = _insert_run_header(conn, status="RUNNING",
                                        skip_schemas=skip_schemas,
                                        exclude_tables=exclude_tables)

        # 2) do the work in a new transaction
        with engine.begin() as conn:
            # discover & filter
            all_targets = _list_all_tables(conn, set(SYSTEM_SCHEMAS) | set(skip_schemas))
            targets = [(s, t) for (s, t) in all_targets if (s, t) not in exclude_pairs]
            if not targets:
                raise ValueError("no tables to process after applying exclusions")

            # count & build structures
            measurements: List[Tuple[int, str, str, int]] = []
            payload: Dict[str, Dict[str, int]] = {}
            for (schema, table) in targets:
                cnt = _count_exact(conn, schema, table)
                measurements.append((run_id, schema, table, cnt))
                payload.setdefault(schema, {})[table] = cnt

            # write detail + snapshots
            _insert_measurements(conn, measurements)
            _write_run_snapshot(conn, run_id, payload)
            _write_daily_snapshot(conn, run_id, business_date, payload)

            # mark success
            _update_run_status(conn, run_id, "SUCCEEDED", None)

        return {
            "status": "ok",
            "runId": run_id,
            "businessDate": str(business_date),
            "tablesProcessed": sum(len(v) for v in payload.values()),
        }

    except Exception as e:
        logging.exception("table metrics job failed")
        # try to mark FAILED if we did create a header
        try:
            if run_id is not None:
                with engine.begi
