Great question. Put them in a **package**, not just a loose folder. In Python, that means a directory with an `__init__.py`. This keeps imports clean, avoids sys.path hacks, and works smoothly on Cloud Functions.

Here’s a tidy layout you can copy:

```
.
├─ main.py                     # Cloud Function entrypoint (cloudevent)
├─ requirements.txt
├─ util/                       # ← your reusable package
│  ├─ __init__.py
│  ├─ config.py                # env-driven names/tz
│  ├─ tables.py                # sa.table(...) builders
│  ├─ db.py                    # engine creator (ConnectionManager → engine)
│  ├─ dml.py                   # inserts/updates/upserts helpers
│  ├─ events.py                # parse/validate cloudevent payload
│  └─ discovery.py             # list tables, count helpers
└─ (optional tests/, etc.)
```

## Why a package (with `__init__.py`)?

* Lets you do `from util.config import load_settings` without fiddling with `sys.path`.
* Keeps code modular/unit-testable.
* Works out-of-the-box with Cloud Functions (it zips the root; Python sees `util` as a package).

---

## Minimal file contents

### util/**init**.py

```python
# util/__init__.py
from .config import load_settings
from .tables import make_tables
from .db import make_engine
from .events import parse_event_body, validate_request
from .discovery import list_all_tables, count_exact
from .dml import (
    insert_run_header, update_run_status,
    insert_measurements, write_run_snapshot, write_daily_snapshot,
)
__all__ = [
    "load_settings", "make_tables", "make_engine",
    "parse_event_body", "validate_request",
    "list_all_tables", "count_exact",
    "insert_run_header", "update_run_status",
    "insert_measurements", "write_run_snapshot", "write_daily_snapshot",
]
```

### util/config.py

```python
import os
DEFAULT_SCHEMA = "nms"
DEFAULT_TABLES = {
    "runs": "db_metrics_runs",
    "metrics": "db_metrics",
    "run_snapshot": "db_metrics_run_snapshot",
    "daily_snapshot": "db_metrics_daily_snapshot",
}
def load_settings():
    schema = os.getenv("DB_SCHEMA", DEFAULT_SCHEMA)
    tables = {
        "runs": os.getenv("TBL_DB_METRICS_RUNS", DEFAULT_TABLES["runs"]),
        "metrics": os.getenv("TBL_DB_METRICS", DEFAULT_TABLES["metrics"]),
        "run_snapshot": os.getenv("TBL_DB_METRICS_RUN_SNAPSHOT", DEFAULT_TABLES["run_snapshot"]),
        "daily_snapshot": os.getenv("TBL_DB_METRICS_DAILY_SNAPSHOT", DEFAULT_TABLES["daily_snapshot"]),
    }
    tz = os.getenv("SNAPSHOT_TZ", "America/New_York")
    return schema, tables, tz
```

### util/tables.py (uses `sa.table()/sa.column()`)

```python
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from types import SimpleNamespace

def make_tables(schema: str, names: dict):
    runs = sa.table(names["runs"],
        sa.column("run_id", sa.BigInteger),
        sa.column("collected_at", sa.TIMESTAMP(timezone=True)),
        sa.column("snapshot_date", sa.Date),
        sa.column("status", sa.Text),
        sa.column("error_message", sa.Text),
        sa.column("skip_schemas", JSONB),
        sa.column("exclude_tables", JSONB),
        schema=schema,
    )
    metrics = sa.table(names["metrics"],
        sa.column("run_id", sa.BigInteger),
        sa.column("schema_name", sa.Text),
        sa.column("table_name", sa.Text),
        sa.column("row_count", sa.BigInteger),
        schema=schema,
    )
    run_snap = sa.table(names["run_snapshot"],
        sa.column("run_id", sa.BigInteger),
        sa.column("payload", JSONB),
        sa.column("collected_at", sa.TIMESTAMP(timezone=True)),
        schema=schema,
    )
    day_snap = sa.table(names["daily_snapshot"],
        sa.column("snapshot_date", sa.Date),
        sa.column("run_id", sa.BigInteger),
        sa.column("payload", JSONB),
        sa.column("collected_at", sa.TIMESTAMP(timezone=True)),
        schema=schema,
    )
    return SimpleNamespace(runs=runs, metrics=metrics, run_snap=run_snap, day_snap=day_snap)
```

### util/db.py

```python
import sqlalchemy as sa
from sqlalchemy.pool import NullPool
from .connections import ConnectionManager  # adjust import path if needed

def _dbapi_creator():
    cm = ConnectionManager()
    if hasattr(cm, "get_connection"): return cm.get_connection()
    if hasattr(cm, "connect"):        return cm.connect()
    return cm()

def make_engine(url: str | None = None) -> sa.Engine:
    # default to pg8000 unless you know you're on psycopg
    url = url or "postgresql+pg8000://"
    return sa.create_engine(url, creator=_dbapi_creator, poolclass=NullPool, future=True)
```

### util/events.py

```python
import json, base64
from cloudevents.http import CloudEvent

def parse_event_body(cloud_event: CloudEvent) -> dict:
    data = cloud_event.data
    if isinstance(data, dict) and "message" in data and isinstance(data["message"], dict):
        b64 = data["message"].get("data", "") or ""
        return json.loads(base64.b64decode(b64).decode("utf-8") or "{}") if b64 else {}
    if isinstance(data, dict): return data
    if isinstance(data, (bytes, str)):
        s = data.decode("utf-8") if isinstance(data, bytes) else data
        try: return json.loads(s)
        except Exception: return {}
    return {}

def validate_request(body: dict):
    exclude, skip = [], []
    for fq in body.get("exclude", []) or []:
        if not isinstance(fq, str) or "." not in fq:
            raise ValueError(f"exclude items must be 'schema.table': {fq}")
        exclude.append(fq)
    skip = list(body.get("skipSchemas", []) or [])
    return exclude, skip
```

### util/discovery.py

```python
import sqlalchemy as sa
SYSTEM_SCHEMAS = {"pg_catalog", "information_schema", "pg_toast"}

def qi(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'

def list_all_tables(conn: sa.Connection, exclude_schemas: set):
    res = conn.execute(sa.text("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type='BASE TABLE'
          AND table_schema NOT IN ('pg_catalog','information_schema','pg_toast')
    """))
    return [(s, t) for (s, t) in res.fetchall() if s not in exclude_schemas]

def count_exact(conn: sa.Connection, schema: str, table: str) -> int:
    fq = f"{qi(schema)}.{qi(table)}"
    return int(conn.execute(sa.text(f"SELECT COUNT(*) FROM {fq}")).scalar_one())
```

### util/dml.py

```python
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert, JSONB
from typing import Dict, List, Tuple, Optional

def insert_run_header(conn: sa.Connection, runs, *, status: str, skip_schemas, exclude_tables) -> int:
    stmt = sa.insert(runs).values(
        status=status, skip_schemas=skip_schemas, exclude_tables=exclude_tables
    ).returning(runs.c.run_id)
    return int(conn.execute(stmt).scalar_one())

def update_run_status(conn: sa.Connection, runs, run_id: int, status: str, error_message: Optional[str]):
    conn.execute(sa.update(runs).where(runs.c.run_id==run_id).values(status=status, error_message=error_message))

def insert_measurements(conn: sa.Connection, metrics, rows: List[Tuple[int, str, str, int]]):
    rows_to_insert = [{"run_id": rid, "schema_name": s, "table_name": t, "row_count": cnt}
                      for (rid, s, t, cnt) in rows]
    if rows_to_insert:
        conn.execute(sa.insert(metrics).values(rows_to_insert))

def write_run_snapshot(conn: sa.Connection, run_snap, run_id: int, payload_nested: Dict[str, Dict[str, int]]):
    stmt = pg_insert(run_snap).values(run_id=run_id, payload=payload_nested)
    conn.execute(stmt.on_conflict_do_update(
        index_elements=[run_snap.c.run_id],
        set_={"payload": stmt.excluded.payload, "collected_at": sa.func.now()},
    ))

def write_daily_snapshot(conn: sa.Connection, day_snap, run_id: int, snapshot_date, payload_nested):
    stmt = pg_insert(day_snap).values(
        snapshot_date=snapshot_date, run_id=run_id, payload=payload_nested
    ).on_conflict_do_update(
        index_elements=[day_snap.c.snapshot_date],
        set_={
            "run_id": sa.case(
                (day_snap.c.run_id.is_(None), sa.cast(sa.literal(run_id), sa.BigInteger)),
                else_=sa.func.greatest(day_snap.c.run_id, sa.cast(sa.literal(run_id), sa.BigInteger)),
            ),
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
```

---

## main.py (now using the package)

```python
# main.py
import datetime, json, logging
from zoneinfo import ZoneInfo
import sqlalchemy as sa
from cloudevents.http import CloudEvent

from util import (
  load_settings, make_tables, make_engine,
  parse_event_body, validate_request,
  list_all_tables, count_exact,
  insert_run_header, update_run_status,
  insert_measurements, write_run_snapshot, write_daily_snapshot,
)

SCHEMA, TBL_NAMES, SNAPSHOT_TZ = load_settings()
T = make_tables(SCHEMA, TBL_NAMES)
ENGINE = make_engine()

SYSTEM_SCHEMAS = {"pg_catalog","information_schema","pg_toast"}

def main(cloud_event: CloudEvent):
    body = parse_event_body(cloud_event)
    business_date = datetime.datetime.now(ZoneInfo(SNAPSHOT_TZ)).date()
    try:
        exclude_tables, skip_schemas = validate_request(body)
        exclude_pairs = {tuple(fq.split(".", 1)) for fq in exclude_tables}
        with ENGINE.begin() as conn:
            run_id = insert_run_header(conn, T.runs, status="RUNNING",
                                       skip_schemas=skip_schemas, exclude_tables=exclude_tables)
            all_targets = list_all_tables(conn, set(SYSTEM_SCHEMAS) | set(skip_schemas))
            targets = [(s, t) for (s, t) in all_targets if (s, t) not in exclude_pairs]
            if not targets:
                raise ValueError("no tables to process after applying exclusions")
            measurements = []
            payload = {}
            for (schema, table) in targets:
                cnt = count_exact(conn, schema, table)
                measurements.append((run_id, schema, table, cnt))
                payload.setdefault(schema, {})[table] = cnt
            insert_measurements(conn, T.metrics, measurements)
            write_run_snapshot(conn, T.run_snap, run_id, payload)
            write_daily_snapshot(conn, T.day_snap, run_id, business_date, payload)
            update_run_status(conn, T.runs, run_id, "SUCCEEDED", None)
        return {"status":"ok","runId":run_id,"businessDate":str(business_date),
                "tablesProcessed":sum(len(v) for v in payload.values())}
    except Exception as e:
        logging.exception("db metrics job failed (atomic rollback)")
        return {"status":"error","message":str(e)}
```

---

## Is “util” a package or just a dir?

Make it a **package** (add `__init__.py`). That’s the “correct way” for Python projects and makes Cloud Functions import it cleanly. If you’re worried about name collisions, consider a more specific package name like `metrics_util` or `nms_util`.

If you want, I can zip this structure into a minimal sample repo layout you can drop into your project.





-----------------------------------------------------------------------------------------------------------



Good catch — that warning is about **SQL injection via string-built SQL**. The only place we were doing that was the `COUNT(*)` call that interpolated `schema.table` into a `text()` string.

Below is a **drop-in fix** that avoids text SQL entirely for dynamic identifiers and uses **SQLAlchemy Core constructs** (which handle quoting safely). I’m also including an optional identifier whitelist if your static analyzer wants it.

---

# ✅ Replace `util/discovery.py`

```python
# util/discovery.py
import re
import sqlalchemy as sa

SYSTEM_SCHEMAS = {"pg_catalog", "information_schema", "pg_toast"}

# Optional: strict identifier whitelist (adjust if you use mixed case or spaces)
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

def _is_safe_ident(name: str) -> bool:
    # If you have exotic names (mixed case/quotes), you can return True
    # and rely purely on SQLAlchemy quoting. Keeping the regex helps many linters.
    return bool(_IDENT_RE.match(name))

def _quoted_table(schema: str, table: str) -> sa.Table:
    """
    Build a SQLAlchemy Table object for COUNT(*), with safe quoting of identifiers.
    No reflection needed; no raw SQL string interpolation.
    """
    if not (_is_safe_ident(schema) and _is_safe_ident(table)):
        # If you truly allow exotic names, you can remove the check above.
        # Using quoted_name forces quoting regardless of content.
        schema = sa.sql.quoted_name(schema, True)
        table  = sa.sql.quoted_name(table,  True)

    # No columns required just to COUNT(*)
    return sa.Table(table, sa.MetaData(), schema=schema)

def list_all_tables(conn: sa.Connection, exclude_schemas: set):
    """
    Returns [(schema, table), ...] from information_schema.
    No untrusted interpolation in the SQL.
    """
    rows = conn.execute(sa.text("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
          AND table_schema NOT IN ('pg_catalog','information_schema','pg_toast')
    """)).all()
    return [(s, t) for (s, t) in rows if s not in exclude_schemas]

def count_exact(conn: sa.Connection, schema: str, table: str) -> int:
    """
    Exact COUNT(*) using Core:
      SELECT count(*) FROM "schema"."table"
    Safe for dynamic identifiers without building SQL strings.
    """
    tbl = _quoted_table(schema, table)
    stmt = sa.select(sa.func.count()).select_from(tbl)
    return int(conn.execute(stmt).scalar_one())
```

### Why this silences the warning

* We **don’t** build `SELECT COUNT(*) FROM {schema}.{table}` strings.
* We let SQLAlchemy **compile and quote** identifiers correctly (safe even if names require quoting).
* Optional `_IDENT_RE` check keeps static analyzers happy; if your org has only simple identifiers, keep it. If you do have exotic names, leave the `quoted_name` fallback (still safe).

---

# dml.py — nothing to change

Your DML helpers already use **parameterized Core** and PostgreSQL **`insert(..).on_conflict_do_update`**. There’s no string interpolation there, so they’re safe.

(If you previously had any `exec_driver_sql("... %s ...", params)`, replace those with Core expressions too.)

---

# main.py — no interface changes

You don’t have to change your main flow; it still calls:

```python
all_targets = list_all_tables(conn, set(SYSTEM_SCHEMAS) | set(skip_schemas))
cnt = count_exact(conn, schema, table)
```

Everything else (bulk insert, upserts, single-transaction commit) stays the same.

---

## Quick sanity test

```python
with ENGINE.begin() as conn:
    # Should print a reasonable count and not trigger the analyzer.
    print(count_exact(conn, "public", "orders"))
```

If you want me to run through any other lines the scanner flagged, paste the exact snippets and I’ll refactor them into safe Core constructs as well.



Great callout. The warnings are about **SQL injection** risk; the fix is to avoid any raw SQL strings and rely on **SQLAlchemy Core** with **bound parameters**. Your `dml.py` can stay 100% safe by:

* Using `sa.insert/sa.update` (no `text()` or string concatenation)
* Using **PostgreSQL upserts** and referencing **`EXCLUDED`** values (no `sa.literal(...)`)
* Passing Python dicts as parameter sets (driver binds them safely)

Here’s a **drop-in, safer `dml.py`** that should silence those scanner findings.

```python
# util/dml.py
from typing import Dict, List, Tuple, Optional
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert

# All functions accept SQLAlchemy Core "table" objects built in util/tables.py
# and a SQLAlchemy Connection (within a transaction, e.g., with engine.begin()).

def insert_run_header(
    conn: sa.Connection,
    runs,
    *,
    status: str,
    skip_schemas: List[str],
    exclude_tables: List[str],
) -> int:
    """
    INSERT into runs and return run_id.
    Uses bound parameters (safe); DB fills collected_at/snapshot_date via defaults.
    """
    stmt = sa.insert(runs).values(
        status=status,
        skip_schemas=skip_schemas,
        exclude_tables=exclude_tables,
    ).returning(runs.c.run_id)

    run_id = conn.execute(stmt).scalar_one()
    return int(run_id)


def update_run_status(
    conn: sa.Connection,
    runs,
    run_id: int,
    status: str,
    error_message: Optional[str],
) -> None:
    """
    UPDATE runs status/error_message using bound parameters.
    """
    stmt = (
        sa.update(runs)
        .where(runs.c.run_id == sa.bindparam("p_run_id"))
        .values(status=sa.bindparam("p_status"), error_message=sa.bindparam("p_err"))
    )
    conn.execute(stmt, {"p_run_id": run_id, "p_status": status, "p_err": error_message})


def insert_measurements(
    conn: sa.Connection,
    metrics,
    rows: List[Tuple[int, str, str, int]],
) -> None:
    """
    Bulk insert per-table counts:
      rows = [(run_id, schema_name, table_name, row_count), ...]
    Emits a single INSERT ... VALUES (...), (...), ... with bound params.
    """
    if not rows:
        return

    rows_to_insert = [
        {"run_id": rid, "schema_name": s, "table_name": t, "row_count": cnt}
        for (rid, s, t, cnt) in rows
    ]

    stmt = sa.insert(metrics).values(rows_to_insert)
    conn.execute(stmt)


def write_run_snapshot(
    conn: sa.Connection,
    run_snap,
    run_id: int,
    payload_nested: Dict[str, Dict[str, int]],
) -> None:
    """
    Upsert per-run JSON snapshot using EXCLUDED values (no literals).
    """
    stmt = pg_insert(run_snap).values(
        run_id=run_id,
        payload=payload_nested,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=[run_snap.c.run_id],
        set_={
            "payload": stmt.excluded.payload,
            "collected_at": sa.func.now(),
        },
    )
    conn.execute(stmt)


def write_daily_snapshot(
    conn: sa.Connection,
    day_snap,
    run_id: int,
    snapshot_date,  # date
    payload_nested: Dict[str, Dict[str, int]],
) -> None:
    """
    Upsert one JSON row per business day:
      - Insert (date, run_id, payload) if not present
      - On conflict, overwrite ONLY if incoming EXCLUDED.run_id is newer
    All comparisons/assignments use EXCLUDED and bound params (no literals).
    """
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
        where=sa.or_(
            day_snap.c.run_id.is_(None),
            stmt.excluded.run_id > day_snap.c.run_id,
        ),
    )

    conn.execute(stmt)
```

### Why this addresses the scanner warnings

* **No raw SQL strings** or identifier interpolation.
* **No `exec_driver_sql(...)`**; everything is Core with bound params.
* The **“newer wins”** logic for the daily snapshot uses `EXCLUDED.run_id` and `EXCLUDED.payload` directly—no `sa.literal(...)`, so linters won’t flag “inlined” values.
* Bulk inserts for measurements use a list of dicts → **parameterized executemany/multi-VALUES**.

### Usage reminder (commit)

Wrap your call site in a single transaction so writes are committed:

```python
with ENGINE.begin() as conn:
    run_id = insert_run_header(conn, T.runs, status="RUNNING",
                               skip_schemas=skip_schemas, exclude_tables=exclude_tables)
    # ... counts ...
    insert_measurements(conn, T.metrics, measurements)
    write_run_snapshot(conn, T.run_snap, run_id, payload)
    write_daily_snapshot(conn, T.day_snap, run_id, business_date, payload)
    update_run_status(conn, T.runs, run_id, "SUCCEEDED", None)
```

If your static analyzer flags anything else, paste the exact line and I’ll refactor it into Core constructs as well.



