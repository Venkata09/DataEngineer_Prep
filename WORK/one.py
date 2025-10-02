# main.py
import os
import json
import base64
import logging
import datetime
from typing import Dict, List, Tuple, Optional
from zoneinfo import ZoneInfo
from contextlib import contextmanager

from cloudevents.http import CloudEvent
from .connections import Connection, ConnectionManager  # your project's manager

# ————————————————————————————————————————————————————————————————
# Config / conventions
# ————————————————————————————————————————————————————————————————
SYSTEM_SCHEMAS = {"pg_catalog", "information_schema", "pg_toast"}
SNAPSHOT_TZ = os.environ.get("SNAPSHOT_TZ", "America/New_York")  # business-day zone for logs/responses


# ————————————————————————————————————————————————————————————————
# DB helpers (uses your ConnectionManager)
# ————————————————————————————————————————————————————————————————
def _get_connection() -> Connection:
    cm = ConnectionManager()
    if hasattr(cm, "get_connection"):
        return cm.get_connection()
    if hasattr(cm, "connect"):
        return cm.connect()
    return cm()  # if your manager is callable

@contextmanager
def db_conn():
    """Commit on success, rollback on error, always close."""
    conn = _get_connection()
    try:
        yield conn
        try:
            conn.commit()
        except Exception:
            pass
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ————————————————————————————————————————————————————————————————
# SQL helpers
# ————————————————————————————————————————————————————————————————
def _quote_ident(ident: str) -> str:
    """Minimal identifier quoting for Postgres."""
    return '"' + ident.replace('"', '""') + '"'

def _list_all_tables(conn: Connection, exclude_schemas: set) -> List[Tuple[str, str]]:
    sql = """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
          AND table_schema NOT IN ('pg_catalog','information_schema','pg_toast')
    """
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    return [(s, t) for (s, t) in rows if s not in exclude_schemas]

def _count_exact(conn: Connection, schema: str, table: str) -> int:
    fq = f"{_quote_ident(schema)}.{_quote_ident(table)}"
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {fq}")
    rc = cur.fetchone()[0]
    cur.close()
    return int(rc)


# ————————————————————————————————————————————————————————————————
# Writes (table_metrics_*)
# ————————————————————————————————————————————————————————————————
def _insert_run_header(conn: Connection, status: str,
                       skip_schemas: List[str], exclude_tables: List[str]) -> int:
    """
    Create a run header and return run_id.
    status: 'RUNNING' | 'SUCCEEDED' | 'FAILED'
    """
    sql = """
      INSERT INTO nms.table_metrics_runs (status, skip_schemas, exclude_tables)
      VALUES (%s, %s::jsonb, %s::jsonb)
      RETURNING run_id
    """
    cur = conn.cursor()
    cur.execute(sql, (status, json.dumps(skip_schemas), json.dumps(exclude_tables)))
    run_id = cur.fetchone()[0]
    cur.close()
    return int(run_id)

def _update_run_status(conn: Connection, run_id: int, status: str, error_message: Optional[str]):
    sql = "UPDATE nms.table_metrics_runs SET status=%s, error_message=%s WHERE run_id=%s"
    cur = conn.cursor()
    cur.execute(sql, (status, error_message, run_id))
    cur.close()

def _insert_measurements(conn: Connection, rows: List[Tuple[int, str, str, int]]) -> None:
    """
    rows: list of (run_id, schema_name, table_name, row_count)
    """
    sql = """
      INSERT INTO nms.table_metrics_measurements (run_id, schema_name, table_name, row_count)
      VALUES (%s, %s, %s, %s)
    """
    cur = conn.cursor()
    cur.executemany(sql, rows)
    cur.close()

def _write_run_snapshot(conn: Connection, run_id: int, payload_nested: Dict[str, Dict[str, int]]) -> None:
    """
    Append-only per-run snapshot (one row per execution).
    """
    sql = """
      INSERT INTO nms.table_metrics_run_snapshot (run_id, payload)
      VALUES (%s, %s::jsonb)
      ON CONFLICT (run_id) DO UPDATE
        SET payload=EXCLUDED.payload, collected_at=now()
    """
    cur = conn.cursor()
    cur.execute(sql, (run_id, json.dumps(payload_nested, separators=(",", ":"))))
    cur.close()

def _write_daily_snapshot(conn: Connection, run_id: int, snapshot_date: datetime.date,
                          payload_nested: Dict[str, Dict[str, int]]) -> None:
    """
    One row per business day; only update if the incoming run_id is newer.
    """
    sql = """
      INSERT INTO nms.table_metrics_daily_snapshot (snapshot_date, run_id, payload)
      VALUES (%s, %s, %s::jsonb)
      ON CONFLICT (snapshot_date) DO UPDATE
         SET run_id       = EXCLUDED.run_id,
             payload      = EXCLUDED.payload,
             collected_at = now()
       WHERE nms.table_metrics_daily_snapshot.run_id IS NULL
          OR EXCLUDED.run_id > nms.table_metrics_daily_snapshot.run_id
    """
    cur = conn.cursor()
    cur.execute(sql, (snapshot_date, run_id, json.dumps(payload_nested, separators=(",", ":"))))
    cur.close()


# ————————————————————————————————————————————————————————————————
# Event parsing / validation (inverse contract)
# ————————————————————————————————————————————————————————————————
def _parse_event_body(cloud_event: CloudEvent) -> dict:
    """
    Accepts:
      - Eventarc Pub/Sub envelope: data.message.data (base64 JSON)
      - Direct CloudEvent with dict/string JSON in .data
    """
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


# ————————————————————————————————————————————————————————————————
# CloudEvent entrypoint
# ————————————————————————————————————————————————————————————————
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

        with db_conn() as conn:
            # 1) mark run as RUNNING (get run_id)
            run_id = _insert_run_header(
                conn,
                status="RUNNING",
                skip_schemas=skip_schemas,
                exclude_tables=exclude_tables,
            )

            # 2) discover tables and apply exclusions
            all_targets = _list_all_tables(conn, set(SYSTEM_SCHEMAS) | set(skip_schemas))
            targets = [(s, t) for (s, t) in all_targets if (s, t) not in exclude_pairs]
            if not targets:
                raise ValueError("no tables to process after applying exclusions")

            # 3) count → measurements + nested JSON
            measurements: List[Tuple[int, str, str, int]] = []
            payload: Dict[str, Dict[str, int]] = {}
            for (schema, table) in targets:
                rc = _count_exact(conn, schema, table)
                measurements.append((run_id, schema, table, rc))
                payload.setdefault(schema, {})[table] = rc

            # 4) write detail + both snapshots
            _insert_measurements(conn, measurements)
            _write_run_snapshot(conn, run_id, payload)
            _write_daily_snapshot(conn, run_id, business_date, payload)

            # 5) success
            _update_run_status(conn, run_id, "SUCCEEDED", None)

        return {
            "status": "ok",
            "runId": run_id,
            "businessDate": str(business_date),
            "tablesProcessed": sum(len(v) for v in payload.values()),
        }

    except Exception as e:
        logging.exception("table metrics job failed")
        # If run header exists, mark FAILED
        try:
            if run_id is not None:
                with db_conn() as conn:
                    _update_run_status(conn, run_id, "FAILED", str(e)[:4000])
            else:
                # couldn't create header; best effort: create a FAILED header for audit
                with db_conn() as conn:
                    _ = _insert_run_header(
                        conn,
                        status="FAILED",
                        skip_schemas=body.get("skipSchemas", []) if isinstance(body, dict) else [],
                        exclude_tables=body.get("exclude", []) if isinstance(body, dict) else [],
                    )
        except Exception:
            pass
        return {"status": "error", "message": str(e)}
