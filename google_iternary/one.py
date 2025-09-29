import os, atexit, json, base64, logging, datetime
from zoneinfo import ZoneInfo
from typing import Dict, List, Tuple, Optional

from cloudevents.http import CloudEvent                     # CloudEvent signature
from sqlalchemy import create_engine, text, MetaData, Table, select, func
from google.cloud.alloydb.connector import Connector, IPTypes
from google.cloud import secretmanager

# ─────────────────────────────
# Environment (configure via deploy flags)
# ─────────────────────────────
INSTANCE_URI    = os.environ["ALLOYDB_INSTANCE_URI"]   # projects/.../instances/...
DB_NAME         = os.environ["DB_NAME"]
DB_USER         = os.environ["DB_USER"]

DB_PASS         = os.environ.get("DB_PASS", "")        # optional if SECRET_RESOURCE or IAM_AUTH
SECRET_RESOURCE = os.environ.get("SECRET_RESOURCE", "")# projects/.../secrets/.../versions/latest
IAM_AUTH        = os.environ.get("IAM_AUTH", "false").lower() == "true"
IP_TYPE         = os.environ.get("IP_TYPE", "PRIVATE").upper()     # PRIVATE | PUBLIC

SNAPSHOT_TZ     = os.environ.get("SNAPSHOT_TZ", "America/New_York") # business day

SYSTEM_SCHEMAS  = {"pg_catalog", "information_schema", "pg_toast"}

# ─────────────────────────────
# Connector + SQLAlchemy engine
# ─────────────────────────────
_connector = Connector()
_secret_client = secretmanager.SecretManagerServiceClient()
_secret_cache: Dict[str, str] = {}
metadata = MetaData()

def _get_secret_payload(res: str) -> str:
    if not res:
        return ""
    if res in _secret_cache:
        return _secret_cache[res]
    val = _secret_client.access_secret_version(name=res).payload.data.decode("utf-8")
    _secret_cache[res] = val
    return val

def _db_password() -> str:
    if IAM_AUTH:
        return ""
    if DB_PASS:
        return DB_PASS
    if SECRET_RESOURCE:
        return _get_secret_payload(SECRET_RESOURCE)
    raise RuntimeError("No DB password. Set DB_PASS or SECRET_RESOURCE, or set IAM_AUTH=true.")

def _getconn():
    ip = IPTypes.PRIVATE if IP_TYPE == "PRIVATE" else IPTypes.PUBLIC
    if IAM_AUTH:
        return _connector.connect(INSTANCE_URI, "pg8000", user=DB_USER, db=DB_NAME, enable_iam_auth=True, ip_type=ip)
    return _connector.connect(INSTANCE_URI, "pg8000", user=DB_USER, password=_db_password(), db=DB_NAME, ip_type=ip)

engine = create_engine(
    "postgresql+pg8000://",
    creator=_getconn,
    pool_pre_ping=True,
    pool_recycle=300,
    pool_size=5,
    max_overflow=5,
)

atexit.register(lambda: _connector.close())

# ─────────────────────────────
# Helpers
# ─────────────────────────────
_reflect_cache: Dict[Tuple[str, str], Table] = {}

def _reflect(schema: str, table: str) -> Table:
    key = (schema, table)
    t = _reflect_cache.get(key)
    if t is None:
        t = Table(table, metadata, schema=schema, autoload_with=engine)
        _reflect_cache[key] = t
    return t

def _list_all_tables(conn, exclude_schemas: set) -> List[Tuple[str, str]]:
    rows = conn.execute(text("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type='BASE TABLE'
          AND table_schema NOT IN ('pg_catalog','information_schema','pg_toast')
    """)).all()
    return [(r.table_schema, r.table_name) for r in rows if r.table_schema not in exclude_schemas]

def _count_exact(conn, schema: str, table: str) -> int:
    t = _reflect(schema, table)
    return conn.execute(select(func.count()).select_from(t)).scalar_one()

def _upsert_daily(conn, snapshot_date: datetime.date, status: str, payload: Dict, error_message: Optional[str]):
    payload_json = json.dumps(payload, separators=(",", ":"))
    conn.execute(
        text("""
            INSERT INTO nms.nms_daily_reporting (snapshot_date, status, payload, error_message, generated_at)
            VALUES (:d, :s, CAST(:p AS jsonb), :e, now())
            ON CONFLICT (snapshot_date)
            DO UPDATE SET
              status       = EXCLUDED.status,
              payload      = EXCLUDED.payload,
              error_message= EXCLUDED.error_message,
              generated_at = now()
        """),
        {"d": snapshot_date, "s": status, "p": payload_json, "e": error_message},
    )

def _parse_event_body(cloud_event: CloudEvent) -> dict:
    """
    Supports:
      - Eventarc Pub/Sub: data={"message":{"data":"<base64 json>"}}
      - Direct CloudEvent with dict/string JSON in .data
    """
    data = cloud_event.data
    # Pub/Sub envelope
    if isinstance(data, dict) and "message" in data and isinstance(data["message"], dict):
        b64 = data["message"].get("data", "") or ""
        if b64:
            decoded = base64.b64decode(b64).decode("utf-8")
            return json.loads(decoded or "{}")
        return {}
    # Direct dict payload
    if isinstance(data, dict):
        return data
    # JSON string/bytes
    if isinstance(data, (bytes, str)):
        s = data.decode("utf-8") if isinstance(data, bytes) else data
        try:
            return json.loads(s)
        except Exception:
            return {}
    return {}

def _validate_request(body: dict) -> Tuple[str, List[Tuple[str, str]], set, datetime.date]:
    """
    Validates and normalizes:
      returns (mode, targets_list_when_list_mode, exclude_schemas_set, snapshot_date)
    """
    mode = body.get("mode", "all")
    if mode not in ("all", "list"):
        raise ValueError("mode must be 'all' or 'list'")
    exclude_schemas = set(body.get("excludeSchemas", []))
    # snapshot date
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

# ─────────────────────────────
# CloudEvent entrypoint
# ─────────────────────────────
def main(cloud_event: CloudEvent):
    """
    CloudEvent payload (JSON) expected:
      {
        "mode": "all" | "list",
        "tables": ["schema.table", ...],   # only when mode="list"
        "excludeSchemas": ["tmp","stage"], # optional
        "snapshotDate": "YYYY-MM-DD"       # optional
      }
    """
    body = _parse_event_body(cloud_event)
    try:
        mode, list_targets, exclude_schemas, snapshot_date = _validate_request(body)

        with engine.begin() as conn:
            # Determine targets
            if mode == "all":
                targets = _list_all_tables(conn, SYSTEM_SCHEMAS | exclude_schemas)
            else:
                # Even in 'list' mode, honor excludeSchemas + system schemas
                targets = [(s, t) for (s, t) in list_targets if s not in (SYSTEM_SCHEMAS | exclude_schemas)]

            if not targets:
                raise ValueError("no tables to process after applying excludeSchemas/system filters")

            # Collect counts
            counts: Dict[str, int] = {}
            for (schema, table) in targets:
                rc = _count_exact(conn, schema, table)
                counts[f"{schema}.{table}"] = int(rc)

            # Upsert daily JSON row (SUCCEEDED)
            _upsert_daily(conn, snapshot_date, "SUCCEEDED", counts, None)

        return {"status": "ok", "date": str(snapshot_date), "tablesProcessed": len(counts)}

    except Exception as e:
        logging.exception("nms daily reporting failed")
        # Attempt to record FAILED row (best effort)
        try:
            with engine.begin() as conn:
                # if we couldn't even parse date, fall back to TZ "today"
                safe_date = snapshot_date if isinstance(snapshot_date, datetime.date) \
                    else datetime.datetime.now(ZoneInfo(SNAPSHOT_TZ)).date()
                _upsert_daily(conn, safe_date, "FAILED", {}, str(e)[:4000])
        except Exception:
            pass
        return {"status": "error", "message": str(e)}
