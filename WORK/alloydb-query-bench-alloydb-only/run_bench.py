
import argparse, os
from datetime import datetime
from bench.benchmark import run_bench
from bench.reporters import write_csv, write_markdown, write_gsheet

def main():
    ap = argparse.ArgumentParser(description="AlloyDB/Postgres Query Benchmarker (AlloyDB-only)")
    ap.add_argument("--config", required=True, help="Path to YAML config")
    ap.add_argument("--out_dir", default="out", help="Output directory")
    args = ap.parse_args()

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    df, agg = run_bench(args.config, out_dir=args.out_dir)

    detail_csv = write_csv(df, args.out_dir, ts)
    agg_csv = os.path.join(args.out_dir, f"benchmark_aggregates_{ts}.csv")
    agg.to_csv(agg_csv, index=False)

    md = write_markdown(df, args.out_dir, ts)
    print(f"Wrote detail CSV: {detail_csv}")
    print(f"Wrote aggregate CSV: {agg_csv}")
    print(f"Wrote Markdown summary: {md}")

    sheet_id = os.getenv("GSHEET_ID")
    if sheet_id:
        ok, err = write_gsheet(df, sheet_id, f"bench_{ts}")
        if ok:
            print(f"Uploaded to Google Sheet tab bench_{ts}")
        else:
            print(f"[Sheets] Skipped/failed: {err}")

if __name__ == "__main__":
    main()
