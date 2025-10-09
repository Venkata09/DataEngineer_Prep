
from __future__ import annotations
import time, json, os
from dataclasses import dataclass
from typing import Any
import pandas as pd
import yaml
from sqlalchemy import text
from sqlalchemy.engine import Engine
from .db import build_engine, apply_session_settings

@dataclass
class RunSpec:
    repeats: int = 5
    warmups: int = 2
    statement_timeout_ms: int = 60000
    default_session: dict | None = None

def _load_sql(file_path: str) -> str:
    with open(file_path, "r", encoding="utf-8") as f:
        return f.read()

def load_config(path: str) -> tuple[RunSpec, list[dict]]:
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    run = cfg.get("run", {}) or {}
    spec = RunSpec(
        repeats=int(run.get("repeats", 5)),
        warmups=int(run.get("warmups", 2)),
        statement_timeout_ms=int(run.get("statement_timeout_ms", 60000)),
        default_session=run.get("default_session")
    )
    queries = cfg.get("queries", [])
    if not isinstance(queries, list) or not queries:
        raise ValueError("config must include a non-empty 'queries' list")
    return spec, queries

def _explain_json(conn, sql: str, params: dict | None) -> dict | None:
    e = text("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql)
    r = conn.execute(e, params or {}).fetchone()
    if not r:
        return None
    payload = r[0]
    if isinstance(payload, str):
        try:
            return json.loads(payload)[0]
        except Exception:
            return None
    if isinstance(payload, list):
        return payload[0] if payload else None
    return None

def _extract_explain_metrics(explain: dict | None) -> dict:
    if not explain:
        return {}
    plan = explain.get("Plan", {})
    planning = explain.get("Planning Time", None)
    exec_ms = plan.get("Actual Total Time", None)
    def walk_buffers(node):
        res = {"shared_hit":0,"shared_read":0,"temp_read":0,"temp_write":0}
        if not isinstance(node, dict):
            return res
        res["shared_hit"] += node.get("Shared Hit Blocks", 0) or 0
        res["shared_read"] += node.get("Shared Read Blocks", 0) or 0
        res["temp_read"] += node.get("Temp Read Blocks", 0) or 0
        res["temp_write"] += node.get("Temp Written Blocks", 0) or 0
        for sub in node.get("Plans", []) or []:
            child = walk_buffers(sub)
            for k in res:
                res[k] += child[k]
        return res
    buf_totals = walk_buffers(plan)
    return {
        "plan_ms": planning,
        "exec_ms": exec_ms,
        **buf_totals
    }

def _maybe_set_timeout(conn, ms: int):
    conn.execute(text("SET statement_timeout = :ms"), {"ms": ms})

def _maybe_apply_session(conn, default_session: dict | None, session: dict | None):
    if default_session:
        for k, v in default_session.items():
            conn.execute(text(f"SET {k} = :v"), {"v": v})
    if session:
        for k, v in session.items():
            conn.execute(text(f"SET {k} = :v"), {"v": v})

def run_bench(config_path: str, out_dir: str = "out"):
    spec, queries = load_config(config_path)
    engine: Engine = build_engine()
    rows = []
    os.makedirs(out_dir, exist_ok=True)
    with engine.connect() as conn:
        _maybe_set_timeout(conn, spec.statement_timeout_ms)
        for q in queries:
            name = q.get("name") or "unnamed"
            sql = q.get("sql")
            file = q.get("file")
            params = q.get("params") or {}
            session = q.get("session") or {}

            if file and not sql:
                sql = _load_sql(file)
            if not sql:
                raise ValueError(f"Query '{name}' must have either 'sql' or 'file'.")

            _maybe_apply_session(conn, spec.default_session, session)

            for _ in range(spec.warmups):
                try:
                    conn.execute(text(sql), params).fetchall()
                except Exception:
                    pass

            for i in range(spec.repeats):
                rec = {
                    "query_name": name, "run_index": i,
                    "wall_ms": None, "rows": None,
                    "plan_ms": None, "exec_ms": None,
                    "shared_hit": None, "shared_read": None,
                    "temp_read": None, "temp_write": None,
                    "error": None
                }
                try:
                    t0 = time.perf_counter()
                    res = conn.execute(text(sql), params)
                    data = res.fetchall()
                    t1 = time.perf_counter()
                    rec["wall_ms"] = (t1 - t0) * 1000.0
                    rec["rows"] = len(data)
                    explain = _explain_json(conn, sql, params)
                    em = _extract_explain_metrics(explain)
                    rec.update({k: em.get(k) for k in ["plan_ms","exec_ms","shared_hit","shared_read","temp_read","temp_write"]})
                except Exception as e:
                    rec["error"] = str(e)
                rows.append(rec)

    df = pd.DataFrame(rows)
    agg = df.groupby("query_name", dropna=False).agg(
        runs=("wall_ms","count"),
        ok_runs=("error", lambda s: int((s.isna() | (s == "")).sum())),
        wall_ms_mean=("wall_ms","mean"),
        wall_ms_median=("wall_ms","median"),
        wall_ms_p95=("wall_ms", lambda s: float(s.quantile(0.95)) if s.notna().any() else None),
        wall_ms_std=("wall_ms","std"),
        rows_mean=("rows","mean"),
        plan_ms_mean=("plan_ms","mean"),
        exec_ms_mean=("exec_ms","mean"),
        shared_hit_sum=("shared_hit","sum"),
        shared_read_sum=("shared_read","sum"),
        temp_read_sum=("temp_read","sum"),
        temp_write_sum=("temp_write","sum"),
    ).reset_index()
    return df, agg
