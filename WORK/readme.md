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
