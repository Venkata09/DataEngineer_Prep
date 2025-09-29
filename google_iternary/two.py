import json, base64, logging, datetime
from typing import Dict, List, Tuple, Optional
from zoneinfo import ZoneInfo

from cloudevents.http import CloudEvent

# Your project's DB connection factory
from .connections import Connection, ConnectionManager


# ── Config ────────────────────────────────────────────────────────────────────
SNAPSHOT_TZ = "America/New_York"  # business-day timezone for snapshot_date
SYSTEM_SCHEMAS = {"pg_catalog", "information_schema", "pg_toast"}


# ── Connection helpers ────────────────────────────────────────────────────────
def _acquire_connection() -> Connection:
    """
    Returns a DB-API connection from your project's ConnectionManager.
    Adjust the method name here if your manager exposes a different one.
    """
    cm = ConnectionManager()
    if hasattr(cm, "get_connection"):
        return cm.get_connection()           # preferred if available
    if hasattr(cm, "connect"):
        return cm.connect()
    # last resort: try calling the instance (e.g., context manager style)
    try:
        return cm()  # type: ignore
    except Exception as e:
        raise RuntimeError("Could not acquire connection from ConnectionManager") from e


# ── SQL helpers (safe quoting & basic ops using DB-API) ───────────────────────
def _quote_ident(ident: str) -> str:
    """Safely quote a SQL identifier (schema/table) for Postgres."""
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


def _upsert_daily(conn: Connection, snapshot_date: datetime.date,
                  status: str, payload: Dict[str, int],
                  error_message: Optional[str]) -> None:
    """
    Upsert one row for snapshot_date into nms.nms_daily_reporting.
    Uses DB-API param placeholders (%s) which work for pg8000/psycopg.
    """
    payload_json = json.dumps(payload, separators=(",", ":"))
    sql = """
        INSERT INTO nms.nms_daily_reporting
            (snapshot_date, status, payload, error_message, generated_at)
        VALUES
            (%s, %s, %s::jsonb, %s, now())
        ON CONFLICT (snapshot_date) DO UPDATE SET
            status        = EXCLUDED.status,
            payload       = EXCLUDED.payload,
            error_message = EXCLUDED.error_message,
            generated_at  = now()
    """
    cur = conn.cursor()
    cur.execute(sql, (snapshot_date, status, payload_json, error_message))
    cur.close()


# ── CloudEvent parsing ────────────────────────────────────────────────────────
def _parse_event_body(cloud_event: CloudEvent) -> dict:
    """
    Accepts:
      - Eventarc Pub/Sub envelope: data.message.data (base64 JSON)
      - Direct CloudEvent with dict or JSON string in .data
    """
    data = cloud_event.data
    if isinstance(data, dict) and "message" in data and isinstance(data["message"], dict):
        b64 = data["message"].get("data", "") or ""
        if b64:
            decoded = base64.b64decode(b64).decode("utf-8")
            return json.loads(decoded or "{}")
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


def _validate_request(body: dict) -> Tuple[str, List[Tuple[str, str]], set, datetime.date]:
    """
    Validates & normalizes the input contract:
    {
      "mode": "all" | "list",
      "tables": ["schema.table", ...],     # required when mode="list"
      "excludeSchemas": ["tmp","stage"],   # optional
      "snapshotDate": "YYYY-MM-DD"         # optional
    }
    """
    mode = body.get("mode", "all")
    if mode not in ("all", "list"):
        raise ValueError("mode must be 'all' or 'list'")

    exclude_schemas = set(body.get("excludeSchemas", []))

    snap_str = body.get("snapshotDate")
    if snap_str:
        snapshot_date = datetime.date.fromisoformat(snap_str)
    else:
        snapshot_date = datetime.datetime.now(ZoneInfo(SNAPSHOT_TZ)).date()

    targets: List[Tuple[str, str]] = []
    if mode == "list":
        tables = body.get("tables")
        if not isinstance(tables, list) or not tables:
            raise ValueError("when mode='list', provide non-empty 'tables' array")
        for fq in tables:
            if not isinstance(fq, str) or "." not in fq:
                raise ValueError(f"table must be 'schema.table': {fq}")
            s, t = fq.split(".", 1)
            targets.append((s, t))

    return mode, targets, exclude_schemas, snapshot_date


# ── Cloud Function entrypoint (CloudEvent) ────────────────────────────────────
def main(cloud_event: CloudEvent):
    """
    CloudEvent payload (JSON):
    {
      "mode": "all" | "list",
      "tables": ["schema.table", "..."],   # only when mode="list"
      "excludeSchemas": ["tmp","stage"],   # optional
      "snapshotDate": "YYYY-MM-DD"         # optional
    }
    """
    body = _parse_event_body(cloud_event)

    try:
        mode, list_targets, exclude_schemas, snapshot_date = _validate_request(body)

        conn = _acquire_connection()
        try:
            # Start a transaction explicitly
            # (Assumes autocommit False; if your manager uses autocommit, remove commit/rollback.)
            if mode == "all":
                targets = _list_all_tables(conn, SYSTEM_SCHEMAS | exclude_schemas)
            else:
                targets = [(s, t) for (s, t) in list_targets if s not in (SYSTEM_SCHEMAS | exclude_schemas)]

            if not targets:
                raise ValueError("no tables to process after applying excludeSchemas/system filters")

            counts: Dict[str, int] = {}
            for (schema, table) in targets:
                rc = _count_exact(conn, schema, table)
                counts[f"{schema}.{table}"] = rc

            _upsert_daily(conn, snapshot_date, "SUCCEEDED", counts, None)
            conn.commit()

            return {"status": "ok", "date": str(snapshot_date), "tablesProcessed": len(counts)}

        except Exception as e:
            logging.exception("nms daily reporting failed (inner)")
            try:
                conn.rollback()
            except Exception:
                pass
            # Best-effort FAILED row (separate transaction)
            try:
                _upsert_daily(conn, snapshot_date, "FAILED", {}, str(e)[:4000])
                conn.commit()
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
            return {"status": "error", "date": str(snapshot_date), "message": str(e)}

        finally:
            try:
                conn.close()
            except Exception:
                pass

    except Exception as e:
        logging.exception("nms daily reporting failed (outer)")
        # If we can't even parse date, fall back to business-day "today"
        safe_date = datetime.datetime.now(ZoneInfo(SNAPSHOT_TZ)).date()
        # We can't record to DB here without a connection, so just return error
        return {"status": "error", "date": str(safe_date), "message": str(e)}
