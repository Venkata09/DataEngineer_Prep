
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import os

def build_engine() -> Engine:
    host = os.getenv("PG_HOST", "127.0.0.1")
    port = os.getenv("PG_PORT", "5432")
    db   = os.getenv("PG_DB", "postgres")
    user = os.getenv("PG_USER", "postgres")
    pwd  = os.getenv("PG_PASSWORD", "postgres")
    sslmode = os.getenv("PG_SSLMODE")  # e.g., 'require'
    sslrootcert = os.getenv("PG_SSLROOTCERT")  # optional CA

    params = {
        "host": host,
        "port": port,
        "dbname": db,
        "user": user,
        "password": pwd
    }
    if sslmode:
        params["sslmode"] = sslmode
    if sslrootcert:
        params["sslrootcert"] = sslrootcert

    # Build DSN via query string
    url_params = "&".join([f"{k}={v}" for k,v in params.items()])
    url = f"postgresql+psycopg2:///?{url_params}"
    engine = create_engine(url, pool_pre_ping=True, pool_size=5, max_overflow=5)
    return engine

def apply_session_settings(conn, settings: dict | None):
    if not settings:
        return
    for key, val in settings.items():
        conn.execute(text(f"SET {key} = :val"), {"val": val})
