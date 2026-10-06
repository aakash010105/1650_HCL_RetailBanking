"""Batch orchestrator. Same code runs locally and inside the AWS Glue job.

Local usage:
    python -m pipeline.run_local day1
    python -m pipeline.run_local day2

Environment overrides (used by the Glue job):
    RB_SRC  folder holding the batch's source files
    RB_OUT  folder for outputs (silver, quarantine, gold, control, serving)

Flow per batch (same order as the Step Functions state machine):
    1. Register: copy files, write manifest, verify sha256, take the idempotency lock.
    2. Stage 1: cleanse and SCD2-merge branches, products, customers.
    3. Stage 2: cleanse transactions, referential checks, upsert the fact table.
    4. Reconcile: row counts and reject thresholds. Mark LOADED or FAILED.
    5. Publish: write the serving layer (DuckDB views, KPI CSVs, snapshots).
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from . import config
from .common import AS_OF, DqContext, write_parquet
from .cleanse import (
    cleanse_branches, cleanse_customers, cleanse_products, cleanse_transactions,
    clean_out_branches, clean_out_customers, clean_out_products, clean_out_transactions,
    load_products,
)
from .curate import (
    BRANCH_TRACKED, CUSTOMER_TRACKED, PRODUCT_TRACKED, resolve_sk, scd2_merge, upsert_fact,
)

ROOT = Path(__file__).resolve().parent.parent
SRC = Path(os.environ.get("RB_SRC", ROOT / "Retailbank_SourceData"))
OUT = Path(os.environ.get("RB_OUT", ROOT / "out"))

FACT_KEY = "fact_transaction"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class ControlStore:
    """JSON stand-in for DynamoDB batch_control. create_if_absent mirrors a conditional put."""

    def __init__(self, path: Path):
        self.path = path
        self.items = json.loads(path.read_text()) if path.exists() else {}

    def create_if_absent(self, batch_id: str, item: dict) -> bool:
        if batch_id in self.items:
            return False
        self.items[batch_id] = item
        self.save()
        return True

    def update(self, batch_id: str, **fields):
        self.items[batch_id].update(fields)
        self.save()

    def save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self.items, indent=2, default=str))


def register(batch: dict, control: ControlStore) -> bool:
    """Stage 0: manifest, checksums, and the idempotency lock. Returns False if already loaded."""
    bid = batch["batch_id"]
    existing = control.items.get(bid)
    if existing and existing["status"] == "LOADED":
        print(f"[{bid}] already LOADED, skipping (idempotent replay)")
        return False

    files = []
    bronze_dir = OUT / "bronze" / f"load_type={batch['load_type']}" / f"dt={batch['dt']}" / f"batch_id={bid}"
    bronze_dir.mkdir(parents=True, exist_ok=True)
    for entity, fname in batch["files"].items():
        dst = bronze_dir / fname
        shutil.copy2(SRC / fname, dst)  # raw bytes, unmodified
        files.append({"entity": entity, "key": fname, "sha256": sha256(dst), "bytes": dst.stat().st_size})
    manifest = {"batch_id": bid, "load_type": batch["load_type"], "dt": batch["dt"],
                "cutoff_ts": batch["cutoff_ts"], "schema_version": "1.0", "files": files}
    (bronze_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))

    for f in files:
        if sha256(SRC / f["key"]) != f["sha256"]:
            raise RuntimeError(f"checksum mismatch for {f['key']}")

    if existing is None:
        control.create_if_absent(bid, {"batch_id": bid, "load_type": batch["load_type"],
                                       "status": "RECEIVED", "started_at": now()})
    control.update(bid, status="VALIDATED")
    return True


def write_silver(df: pd.DataFrame, entity: str, bid: str) -> None:
    write_parquet(df, OUT / "silver" / entity / f"{bid}.parquet")


def write_quarantine(ctx: DqContext, bid: str) -> pd.DataFrame:
    rejects = ctx.rejects_frame()
    write_parquet(rejects, OUT / "quarantine" / ctx.entity / f"{bid}.parquet")
    return rejects


def gold_path(name: str) -> Path:
    return OUT / "gold" / name / "part-0000.parquet"


def load_gold(name: str) -> pd.DataFrame | None:
    p = gold_path(name)
    return pd.read_parquet(p) if p.exists() else None


def save_gold(name: str, df: pd.DataFrame) -> None:
    write_parquet(df, gold_path(name))


def stage_dimensions(batch: dict, bid: str) -> dict:
    cutoff = pd.Timestamp(batch["cutoff_ts"]) if batch["load_type"] == "incremental" else None
    results = {}

    # Branches
    raw = pd.read_csv(SRC / batch["files"]["branches"], dtype=str, keep_default_na=False)
    clean, ctx = cleanse_branches(raw, bid, batch["files"]["branches"])
    clean = clean_out_branches(clean)
    write_silver(clean, "branches", bid)
    rej = write_quarantine(ctx, bid)
    dim, summary = scd2_merge(load_gold("dim_branch"), clean, "branch_id", "branch_sk", BRANCH_TRACKED, cutoff)
    save_gold("dim_branch", dim)
    results["branches"] = {"raw": len(raw), "clean": len(clean), "rejected": len(rej),
                           "dq_rejected": len(rej), "scd2": summary}

    # Products (repaired JSON)
    records, repaired = load_products(str(SRC / batch["files"]["products"]))
    if not records:
        ctx = DqContext("products", bid, batch["files"]["products"])
        rej = write_quarantine(ctx, bid)
        results["products"] = {"raw": 0, "clean": 0, "rejected": 0, "dq_rejected": 0, "scd2": None,
                               "error": "FILE_UNPARSEABLE"}
    else:
        clean, ctx = cleanse_products(records, bid, batch["files"]["products"], repaired)
        clean = clean_out_products(clean)
        write_silver(clean, "products", bid)
        rej = write_quarantine(ctx, bid)
        dim, summary = scd2_merge(load_gold("dim_product"), clean, "product_id", "product_sk", PRODUCT_TRACKED, cutoff)
        save_gold("dim_product", dim)
        results["products"] = {"raw": len(records), "clean": len(clean), "rejected": len(rej),
                               "dq_rejected": len(rej), "json_repaired": repaired, "scd2": summary}

    # Customers (dedup, account conflicts, DOB, phone, email, kyc)
    raw = pd.read_csv(SRC / batch["files"]["customers"], dtype=str, keep_default_na=False)
    clean, ctx = cleanse_customers(raw, bid, batch["files"]["customers"])
    clean = clean_out_customers(clean)
    write_silver(clean, "customers", bid)
    rej = write_quarantine(ctx, bid)
    dim, summary = scd2_merge(load_gold("dim_customer"), clean, "customer_id", "customer_sk", CUSTOMER_TRACKED, cutoff)
    save_gold("dim_customer", dim)
    results["customers"] = {"raw": len(raw), "clean": len(clean), "rejected": len(rej),
                            "dq_rejected": len(rej), "scd2": summary}
    return results


def stage_facts(batch: dict, bid: str) -> dict:
    raw = pd.read_csv(SRC / batch["files"]["transactions"], dtype=str, keep_default_na=False)
    clean, ctx = cleanse_transactions(raw, bid, batch["files"]["transactions"])

    cust = load_gold("dim_customer")
    prod = load_gold("dim_product")
    branch = load_gold("dim_branch")

    # Referential integrity against the Gold dimensions (already merged in Stage 1).
    checks = [
        ("account_id", cust, "account_id", "TX-RI-ACCT", "account_id not found in dim_customer"),
        ("product_id", prod, "product_id", "TX-RI-PROD", "product_id not found in dim_product"),
        ("branch_id", branch, "branch_id", "TX-RI-BR", "branch_id not found in dim_branch"),
    ]
    for col, dim, dim_col, rule, detail in checks:
        missing = ~clean[col].isin(set(dim[dim_col]))
        clean = ctx.reject(clean, missing, rule, "MISSING_DIM", detail)

    clean = clean_out_transactions(clean)
    clean["customer_sk"] = resolve_sk(clean, cust, "account_id", "account_id", "customer_sk", "timestamp")
    clean["product_sk"] = resolve_sk(clean, prod, "product_id", "product_id", "product_sk", "timestamp")
    clean["branch_sk"] = resolve_sk(clean, branch, "branch_id", "branch_id", "branch_sk", "timestamp")

    write_silver(clean, "transactions", bid)
    rej = write_quarantine(ctx, bid)

    fact, summary = upsert_fact(load_gold(FACT_KEY), clean, bid)
    save_gold(FACT_KEY, fact)
    dq_rej = int((rej["error_code"] != "MISSING_DIM").sum())
    return {"raw": len(raw), "clean": len(clean), "rejected": len(rej), "dq_rejected": dq_rej,
            "missing_dim": len(rej) - dq_rej, "fact": summary,
            "clean_amount_inr": int(clean.loc[clean["currency"] == "INR", "amount"].sum())}


def reconcile(results: dict) -> list[str]:
    """Row-count checks. Returns a list of failures (empty = pass)."""
    failures = []
    for entity, r in results.items():
        if r.get("error"):
            failures.append(f"{entity}: {r['error']}")
            continue
        if r["raw"] != r["clean"] + r["rejected"]:
            failures.append(f"{entity}: raw {r['raw']} != clean {r['clean']} + rejected {r['rejected']}")
    return failures


def quarantine_rate_check(results: dict) -> list[str]:
    """Fails the batch when an entity's data-quality reject rate exceeds its threshold.
    Referential misses (MISSING_DIM) are excluded because they are expected for late dimensions."""
    breaches = []
    for entity, r in results.items():
        if not r.get("raw"):
            continue
        rate = r["dq_rejected"] / r["raw"]
        limit = config.QUARANTINE_THRESHOLD.get(entity, config.QUARANTINE_THRESHOLD["default"])
        if rate > limit:
            breaches.append(f"{entity}: reject rate {rate:.1%} > threshold {limit:.1%}")
    return breaches


def scorecard(bid: str, batch: dict, results: dict) -> pd.DataFrame:
    """KPI 10 source: received, passed, rejected per file, plus the top rejection reasons."""
    rows = []
    for entity, fname in batch["files"].items():
        rej_path = OUT / "quarantine" / entity / f"{bid}.parquet"
        rej = pd.read_parquet(rej_path) if rej_path.exists() else pd.DataFrame(columns=["error_code"])
        top = rej["error_code"].value_counts().head(3)
        r = results.get(entity, {})
        rows.append({
            "batch_id": bid, "source_file": fname, "entity": entity,
            "received": r.get("raw", 0), "passed": r.get("clean", 0), "rejected": r.get("rejected", 0),
            "top_reasons": "; ".join(f"{k}={v}" for k, v in top.items()),
        })
    return pd.DataFrame(rows)


def run(batch: dict) -> dict:
    """Run one batch end to end. Gold is published all-or-nothing. Returns the batch results."""
    bid = batch["batch_id"]
    (OUT / "control").mkdir(parents=True, exist_ok=True)
    control = ControlStore(OUT / "control" / "batch_control.json")

    if not register(batch, control):
        return control.items[bid].get("results", {})

    control.update(bid, status="PROCESSING")
    gold_backup = OUT / "gold_before_batch"
    if gold_backup.exists():
        shutil.rmtree(gold_backup)
    if (OUT / "gold").exists():
        shutil.copytree(OUT / "gold", gold_backup)
    try:
        results = stage_dimensions(batch, bid)
        results["transactions"] = stage_facts(batch, bid)
        failures = reconcile(results) + quarantine_rate_check(results)
        if failures:
            raise RuntimeError("; ".join(failures))
    except Exception as exc:
        if (OUT / "gold").exists():
            shutil.rmtree(OUT / "gold")
        if gold_backup.exists():
            shutil.copytree(gold_backup, OUT / "gold")
        control.update(bid, status="FAILED", error=str(exc), ended_at=now())
        print(f"[{bid}] FAILED: {exc}")
        raise
    finally:
        if gold_backup.exists():
            shutil.rmtree(gold_backup)

    write_parquet(scorecard(bid, batch, results), OUT / "control" / "dq_scorecard" / f"{bid}.parquet")
    audit = {"batch_id": bid, "as_of": str(AS_OF.date()), "results": results}
    (OUT / "control" / f"batch_audit_{bid}.json").write_text(json.dumps(audit, indent=2, default=str))
    control.update(bid, status="LOADED", ended_at=now(), results=results)
    print(json.dumps(results, indent=2, default=str))

    from . import kpi  # serving layer is published only after the batch is LOADED
    print("KPI rows:", kpi.publish(bid))
    return results


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in config.BATCHES:
        sys.exit(f"usage: python -m pipeline.run_local {{{'|'.join(config.BATCHES)}}}")
    run(config.BATCHES[sys.argv[1]])
