
from __future__ import annotations
import os
import pandas as pd
from tabulate import tabulate

def write_csv(df: pd.DataFrame, out_dir: str, ts: str) -> str:
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"benchmark_results_{ts}.csv")
    df.to_csv(path, index=False)
    return path

def write_markdown(df: pd.DataFrame, out_dir: str, ts: str) -> str:
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"benchmark_summary_{ts}.md")
    cols = [
        "query_name","run_index","wall_ms","rows","plan_ms","exec_ms",
        "shared_hit","shared_read","temp_read","temp_write","error"
    ]
    subset = df[cols] if set(cols).issubset(df.columns) else df
    with open(path, "w", encoding="utf-8") as f:
        f.write("# Benchmark Summary\n\n")
        f.write(tabulate(subset, headers="keys", tablefmt="github", showindex=False))
        f.write("\n")
    return path

def write_gsheet(df: pd.DataFrame, sheet_id: str, tab_name: str):
    try:
        import gspread
        # Uses GOOGLE_APPLICATION_CREDENTIALS if set
        gc = gspread.service_account()
        sh = gc.open_by_key(sheet_id)
        try:
            ws = sh.worksheet(tab_name)
            sh.del_worksheet(ws)
        except Exception:
            pass
        ws = sh.add_worksheet(title=tab_name, rows="100", cols="26")
        ws.update([df.columns.tolist()] + df.values.tolist())
        return True, None
    except Exception as e:
        return False, str(e)
