# main.py
import os
import json
import atexit
import logging
import datetime
from typing import List, Tuple, Dict

from flask import Request, make_response
from sqlalchemy import create_engine, text, MetaData, Table, select, func

from google.cloud.alloydb.connector import Connector, IPTypes
from google.cloud import secretmanager

# ──────────────────────────────────────────────────────────────────────────────
# Env / config
# ──────────────────────────────────────────────────────────────────────────────
# AlloyDB connector expects: projects/<proj>/locations/<region>/clusters/<cluster>/instances/<instance>
INSTANCE_URI = os.environ["ALLOYDB_INSTANCE_URI"]
DB_NAME      = os.environ["DB_NAME"]
DB_USER      = os.environ["DB_USER"]

# Password options:
# 1) Set DB_PASS directly as env, OR
# 2) Provide SECRET_RESOURCE like "projects/<proj>/secrets/<name>/versions/latest"
DB_PASS          = os.environ.get("DB_PASS", "")
SECRET_RESOURCE  = os.environ.get("SECRET_RESOURCE", "")  # optional

# Optional IAM DB auth (no password needed if True and role is configured)
IAM_AUTH   = os.environ.get("IAM_AUTH", "false").lower() == "true"

# Private or Public IP (PRIVATE recommended)
IP_TYPE    = os.environ.get("IP_TYPE", "PRIVATE").upper()

ENVIRONMENT   = os.environ.get("ENVIRONMENT", "sandbox")
SOURCE_SYSTEM = os.environ.get("SOURCE_SYSTEM", "alloydb-cluster-a")

SYSTEM_SCHEMAS = {"pg_catalog", "information_schema", "pg_toast"}

# ──────────────────────────────────────────────────────────────────────────────
# Connector, Secret Manager, SQLAlchemy engine
# ──────────────────────────────────────────────────────────────────────────────
_connector = Connector()  # re-used across invocations
_secret_client = secretmanager.SecretManagerServiceClient()
_secret_cache: Dict[str, str] = {}

def _get_secret_payload(secret_resource: str) -> str:
    if secret_resource in _secret_cache:
        return _secret_cache[secret_resource]
    payload = _secret_client.access_secret_version(name=secret_resource).payload.data.decode("utf-8")
    _secret_cache[secret_resource] = payload
    return payload

def _db_password() -> str:
    if IAM_AUTH:
        return ""  # not used
    if DB_PASS:
        return DB_PASS
    if SECRET_RESOURCE:
        return _get_secret_payload(SECRET_RESOURCE)
    raise RuntimeError("No DB password provided. Set DB_PASS or SECRET_RESOURCE, or enable IAM_AUTH=true.")

def _getconn():
    ip = IPTypes.PRIVATE if IP_TYPE == "PRIVATE" else IPTypes.PUBLIC
    if IAM_AUTH:
        # IAM DB Auth (user must be configured for IAM auth in AlloyDB)
        return _connector.connect(
            INSTANCE_URI,
            "pg8000",
            user=DB_USER,
            db=DB_NAME,
            enable_iam_auth=True,
            ip_type=ip,
        )
    else:
        return _connector.connect(
            INSTANCE_URI,
            "pg8000",
            user=DB_USER,
            password=_db_password(),
            db=DB_NAME,
            ip_type=ip,
        )

# Create engine with "creator" so SQLAlchemy asks connector for each connection
engine = create_engine(
    "postgresql+pg8000://",
    creator=_getconn,
    pool_pre_ping=True,
    pool_recycle=300,
    pool_size=5,
    max_overflow=5,
)
metadata = MetaData()

# Close connector when the instance is torn down
atexit.register(lambda: _connector.close())

# ──────────────────────────────────────────────────────────────────────────────
# Run header helpers
# ──────────────────────────────────────────────────────────────────────────────
def insert_run_header(conn) -> int:
    return conn.execute(
        text("""
            INSERT INTO ops.rowcount_run(environment, source_system)
            VALUES (:env, :src)
            RETURNING run_id
        """),
        {"env": ENVIRONMENT, "src": SOURCE_SYSTEM},
    ).scalar_one()

def update_run_status(conn, run_id: int, status: str, error_message: str | None = None):
    conn.execute(
        text("UPDATE ops.rowcount_run SET status=:s, error_message=:e WHERE run_id=:id"),
        {"s": status, "e": error_message, "id": run_id},
    )

# ──────────────────────────────────────────────────────────────────────────────
# Table discovery
# ──────────────────────────────────────────────────────────────────────────────
def list_tables_from_config(conn, exclude: set) -> List[Tuple[str, str]]:
    rows = conn.execute(
        text("""
            SELECT schema_name, table_name
            FROM ops.rowcount_targets
            WHERE enabled = true
        """)
    ).all()
    return [(r.schema_name, r.table_name) for r in rows if r.schema_name not in exclude]

def list_all_user_tables(conn, exclude: set) -> List[Tuple[str, str]]:
    rows = conn.execute(
        text("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type='BASE TABLE'
              AND table_schema NOT IN ('pg_catalog','information_schema','pg_toast')
        """)
    ).all()
    return [(r.table_schema, r.table_name) for r in rows if r.table_schema not in exclude]

def list_from_payload(tables: List[str], exclude: set) -> List[Tuple[str, str]]:
    out = []
    for fq in tables or []:
        if "." not in fq:
            raise ValueError(f"Use 'schema.table' format: {fq}")
        s, t = fq.split(".", 1)
        if s not in exclude:
            out.append((s, t))
    return out

# ──────────────────────────────────────────────────────────────────────────────
# Sizes + counts
# ──────────────────────────────────────────────────────────────────────────────
def get_sizes(conn, schema: str, table: str) -> Tuple[int, int, int]:
    row = conn.execute(
        text("""
            SELECT
              pg_total_relation_size(c.oid)::bigint AS bytes_total,
              pg_relation_size(c.oid)::bigint       AS bytes_table,
              pg_indexes_size(c.oid)::bigint        AS bytes_index
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = :schema AND c.relname = :table
              AND c.relkind = 'r'
        """),
        {"schema": schema, "table": table},
    ).one_or_none()
    return (row.bytes_total, row.bytes_table, row.bytes_index) if row else (0, 0, 0)

_reflect_cache: Dict[Tuple[str, str], Table] = {}

def _reflect_table(schema: str, table: str) -> Table:
    key = (schema, table)
    t = _reflect_cache.get(key)
    if t is None:
        t = Table(table, metadata, schema=schema, autoload_with=engine)
        _reflect_cache[key] = t
    return t

def count_exact(conn, schema: str, table: str) -> int:
    t = _reflect_table(schema, table)
    return conn.execute(select(func.count()).select_from(t)).scalar_one()

def count_approx(conn, schema: str, table: str) -> int:
    row = conn.execute(
        text("""
            SELECT COALESCE(n_live_tup, 0)::bigint AS est
            FROM pg_stat_user_tables
            WHERE schemaname = :schema AND relname = :table
        """),
        {"schema": schema, "table": table},
    ).one_or_none()
    return int(row.est) if row else 0

def upsert_snapshot(conn, run_id: int, ts: datetime.datetime,
                    schema: str, table: str, row_count: int,
                    sizes: Tuple[int, int, int]):
    bytes_total, bytes_table, bytes_index = sizes
    conn.execute(
        text("""
            INSERT INTO ops.rowcount_snapshot (
              run_id, snapshot_ts, db_name, schema_name, table_name,
              row_count, bytes_total, bytes_table, bytes_index, extra
            )
            VALUES (
              :run_id, :ts, current_database(), :schema, :table,
              :rc, :btot, :btab, :bidx, '{}'::jsonb
            )
            ON CONFLICT (schema_name, table_name, snapshot_date)
            DO UPDATE SET
              run_id      = EXCLUDED.run_id,
              row_count   = EXCLUDED.row_count,
              bytes_total = EXCLUDED.bytes_total,
              bytes_table = EXCLUDED.bytes_table,
              bytes_index = EXCLUDED.bytes_index,
              snapshot_ts = EXCLUDED.snapshot_ts
        """),
        {
            "run_id": run_id, "ts": ts, "schema": schema, "table": table,
            "rc": int(row_count), "btot": int(bytes_total),
            "btab": int(bytes_table), "bidx": int(bytes_index),
        },
    )

# ──────────────────────────────────────────────────────────────────────────────
# Cloud Function entrypoint
# ──────────────────────────────────────────────────────────────────────────────
def main(request: Request):
    """
    POST JSON:
      {
        "mode": "config" | "all" | "list",
        "tables": ["public.orders","sales.customers"],   # when mode=list
        "exact": true,                                   # exact vs approx
        "excludeSchemas": ["tmp","stage"]                # optional
      }
    """
    body = request.get_json(silent=True) or {}
    mode = body.get("mode", "config")
    exact = bool(body.get("exact", True))
    exclude = set(body.get("excludeSchemas", []))
    include_tables = body.get("tables", [])

    ts = datetime.datetime.now(datetime.timezone.utc)
    run_id = None

    try:
        with engine.begin() as conn:
            run_id = insert_run_header(conn)

            if mode == "config":
                targets = list_tables_from_config(conn, exclude)
            elif mode == "all":
                targets = list_all_user_tables(conn, exclude)
            elif mode == "list":
                targets = list_from_payload(include_tables, exclude)
            else:
                raise ValueError(f"Unknown mode: {mode}")

            processed = 0
            for (schema, table) in targets:
                rc = count_exact(conn, schema, table) if exact else count_approx(conn, schema, table)
                sizes = get_sizes(conn, schema, table)
                upsert_snapshot(conn, run_id, ts, schema, table, rc, sizes)
                processed += 1

            update_run_status(conn, run_id, "SUCCEEDED")

        return make_response(
            {"status": "ok", "mode": mode, "run_id": run_id, "tablesProcessed": processed, "exact": exact},
            200,
        )
    except Exception as e:
        logging.exception("rowcount run failed")
        try:
            with engine.begin() as conn:
                if run_id is None:
                    run_id = insert_run_header(conn)
                update_run_status(conn, run_id, "FAILED", str(e))
        except Exception:
            pass
        return make_response({"status": "error", "message": str(e)}, 500)
