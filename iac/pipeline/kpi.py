"""Serving layer: DuckDB database with the Gold tables, one view per KPI, and per-batch snapshots.

Called at the end of every successful batch (publish). The SQL in sql/ is written to run on
DuckDB locally and on Athena (Trino) in AWS, with the same results.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import duckdb
import pandas as pd

from . import config
from .common import AS_OF, write_parquet

ROOT = Path(__file__).resolve().parent.parent
OUT = Path(os.environ.get("RB_OUT", ROOT / "out"))
SQL_DIR = Path(os.environ.get("RB_SQL", ROOT / "sql"))  # Glue sets RB_SQL to the extracted zip
SERVING = OUT / "serving"

KPI_VIEWS = sorted(p.stem for p in SQL_DIR.glob("kpi_*.sql"))


def _sql(name: str) -> str:
    text = (SQL_DIR / f"{name}.sql").read_text(encoding="utf-8").strip().rstrip(";")
    cutoff = config.BATCHES["day2"]["cutoff_ts"].replace("T", " ")
    return text.replace("{as_of}", str(AS_OF.date())).replace("{cutoff}", cutoff)


def _batch_changes() -> pd.DataFrame:
    rows = []
    for path in sorted((OUT / "control").glob("batch_audit_*.json")):
        audit = json.loads(path.read_text())
        r = audit["results"]
        fact = r["transactions"]["fact"]
        rows.append({
            "batch_id": audit["batch_id"],
            "fact_inserted": fact["inserted"],
            "fact_updated": fact["updated"],
            "fact_corrected": fact["corrected"],
            "customers_new": r["customers"]["scd2"]["new"],
            "customers_changed": r["customers"]["scd2"]["changed"],
            "products_changed": (r["products"].get("scd2") or {}).get("changed", 0),
            "branches_changed": r["branches"]["scd2"]["changed"],
        })
    return pd.DataFrame(rows)


def _snapshot(con: duckdb.DuckDBPyConnection, batch_id: str) -> pd.DataFrame:
    """KPI 1 and KPI 7 totals as of this batch. KPI 11 compares them across batches."""
    k1 = con.execute(_sql("kpi_01_top5_customers")).df()
    k7 = con.execute(_sql("kpi_07_branch_rank_region")).df()
    rows = [
        {"batch_id": batch_id, "kpi": "kpi_01_top5_net", "key": str(r.account_id), "metric": float(r.net_volume)}
        for r in k1.itertuples()
    ] + [
        {"batch_id": batch_id, "kpi": "kpi_07_branch_net", "key": str(r.branch_id), "metric": float(r.net_volume)}
        for r in k7.itertuples()
    ]
    return pd.DataFrame(rows)


def publish(batch_id: str) -> dict:
    SERVING.mkdir(parents=True, exist_ok=True)
    db_path = SERVING / "retailbank.duckdb"
    if db_path.exists():
        db_path.unlink()  # rebuilt from Gold each time, so there is no drift
    con = duckdb.connect(str(db_path))

    gold = OUT / "gold"
    con.execute(f"CREATE TABLE fact_transaction AS SELECT * FROM read_parquet('{gold / 'fact_transaction' / 'part-0000.parquet'}')")
    for dim in ["customer", "branch", "product"]:
        path = gold / f"dim_{dim}" / "part-0000.parquet"
        con.execute(f"CREATE TABLE dim_{dim} AS SELECT * FROM read_parquet('{path}')")
    con.execute("""
        CREATE VIEW v_fact AS
        SELECT transaction_id, account_id, product_id, branch_id, customer_sk, product_sk, branch_sk,
               transaction_date, "timestamp" AS txn_ts, amount, status, currency, is_refund,
               payment_method, is_correction, first_batch_id, loaded_batch_id
        FROM fact_transaction
    """)

    scorecards = sorted((OUT / "control" / "dq_scorecard").glob("*.parquet"))
    con.execute("CREATE TABLE dq_scorecard AS SELECT * FROM read_parquet(["
                + ",".join(f"'{p}'" for p in scorecards) + "], union_by_name=true)")

    changes = _batch_changes()
    write_parquet(changes, SERVING / "batch_changes" / "batch_changes.parquet")
    con.execute("CREATE TABLE batch_changes AS SELECT * FROM read_parquet("
                f"'{SERVING / 'batch_changes' / 'batch_changes.parquet'}')")

    snap_path = SERVING / "kpi_snapshot" / "kpi_snapshot.parquet"
    snaps = pd.read_parquet(snap_path) if snap_path.exists() else pd.DataFrame(columns=["batch_id", "kpi", "key", "metric"])
    snaps = pd.concat([snaps[snaps.batch_id != batch_id], _snapshot(con, batch_id)], ignore_index=True)
    write_parquet(snaps, snap_path)
    con.execute("CREATE TABLE kpi_snapshot AS SELECT * FROM read_parquet("
                f"'{snap_path}')")

    for name in KPI_VIEWS:
        con.execute(f"CREATE VIEW {name} AS {_sql(name)}")

    export = SERVING / "kpi_csv"
    export.mkdir(parents=True, exist_ok=True)
    counts = {}
    for name in KPI_VIEWS:
        df = con.execute(f"SELECT * FROM {name}").df()
        df.to_csv(export / f"{name}.csv", index=False)
        counts[name] = len(df)
    con.close()
    return counts


if __name__ == "__main__":
    import sys
    print(publish(sys.argv[1] if len(sys.argv) > 1 else "manual"))
