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

# ── Config ───────────────────────────────────────────────────────────────────
SYSTEM_SCHEMAS = {"pg_catalog", "information_schema", "pg_toast"}
SNAPSHOT_TZ = os.environ.get("SNAPSHOT_TZ", "America/New_York")

# ── Engine (creator=ConnectionManager; no pooling) ───────────────────────────
def _dbapi_creator():
    cm = ConnectionManager()
    if hasattr(cm, "get_connection"): return cm.get_connection()
    if hasattr(cm, "connect"):        return cm.connect()
    return cm()

engine = sa.create_engine(
    "postgresql+pg8000://",      # adjust if your ConnectionManager returns psycopg*
    creator=_dbapi_creator,
    poolclass=NullPool,
    future=True,
)

# ── Table metadata (must match your DDL in schema nms) ───────────────────────
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

# ── DML ops (all execute within ONE transaction in main()) ────────────────────
def _insert_run_header(conn: sa.Connection, status: str,
                       skip_schemas: List[str], exclude_tables: List[str]) -> int:
    # DB defaults fill collected_at/snapshot_date
    stmt = sa.insert(runs).values(
        status=status,
        skip_schemas=skip_schemas,
        exclude_tables=exclude_tables,
    ).returning(runs.c.run_id)
    return int(conn.execute(stmt).scalar_one())

def _update_run_status(conn: sa.Connection, run_id: int, status: str, error_message: Optional[str]):
    conn.execute(
        sa.update(runs)
        .where(runs.c.run_id == run_id)
        .values(status=status, error_message=error_message)
    )

def _insert_measurements(conn: sa.Connection, rows: List[Tuple[int, str, str, int]]) -> None:
    rows_to_insert = [
        {"run_id": rid, "schema_name": s, "table_name": t, "row_count": cnt}
        for (rid, s, t, cnt) in rows
    ]
    if rows_to_insert:
        conn.execute(sa.insert(meas).values(rows_to_insert))

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
    ).on_conflict_do_update(
        index_elements=[day_snap.c.snapshot_date],
        set_={
            "run_id": sa.case(
                (day_snap.c.run_id.is_(None), sa.cast(sa.literal(run_id), sa.BigInteger)),
                else_=sa.func.greatest(day_snap.c.run_id, sa.cast(sa.literal(run_id), sa.BigInteger)),
            ),  # defensive; WHERE guard below still enforces "newer wins"
            "payload": sa.case(
                (sa.or_(day_snap.c.run_id.is_(None), sa.literal(run_id) > day_snap.c.run_id),
                 sa.cast(sa.literal(payload_nested), JSONB)),
                else_=day_snap.c.payload,
            ),
            "collected_at": sa.func.now(),
        },
        where=sa.or_(day_snap.c.run_id.is_(None), sa.literal(run_id) > day_snap.c.run_id),
    )
    conn.execute(stmt)

# ── CloudEvent entrypoint (SINGLE TRANSACTION, atomic) ────────────────────────
def main(cloud_event: CloudEvent):
    """
    Atomic run: header → counts → measurements → snapshots → mark SUCCEEDED.
    If ANY step fails, the transaction is rolled back and NOTHING is written.
    """
    body = _parse_event_body(cloud_event)
    business_date = datetime.datetime.now(ZoneInfo(SNAPSHOT_TZ)).date()

    try:
        exclude_tables, skip_schemas = _validate_request(body)
        exclude_pairs = {tuple(fq.split(".", 1)) for fq in exclude_tables}

        with engine.begin() as conn:  # ← commits on success; rolls back on error
            # 1) header (RUNNING)
            run_id = _insert_run_header(conn, "RUNNING", skip_schemas, exclude_tables)

            # 2) discover targets
            all_targets = _list_all_tables(conn, set(SYSTEM_SCHEMAS) | set(skip_schemas))
            targets = [(s, t) for (s, t) in all_targets if (s, t) not in exclude_pairs]
            if not targets:
                raise ValueError("no tables to process after applying exclusions")

            # 3) count & stage writes
            measurements: List[Tuple[int, str, str, int]] = []
            payload: Dict[str, Dict[str, int]] = {}
            for (schema, table) in targets:
                cnt = _count_exact(conn, schema, table)
                measurements.append((run_id, schema, table, cnt))
                payload.setdefault(schema, {})[table] = cnt

            # 4) write normalized + snapshots
            _insert_measurements(conn, measurements)
            _write_run_snapshot(conn, run_id, payload)
            _write_daily_snapshot(conn, run_id, business_date, payload)

            # 5) success
            _update_run_status(conn, run_id, "SUCCEEDED", None)

        # if we got here, COMMIT happened
        return {
            "status": "ok",
            "runId": run_id,
            "businessDate": str(business_date),
            "tablesProcessed": sum(len(v) for v in payload.values()),
        }

    except Exception as e:
        logging.exception("table metrics job failed (atomic rollback)")
        # No follow-up writes here (to keep atomicity). Nothing is committed.
        return {"status": "error", "message": str(e)}
