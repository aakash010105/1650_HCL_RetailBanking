"""Gold layer: SCD Type 2 dimensions, fact upsert, and surrogate-key resolution."""
from __future__ import annotations

import pandas as pd

from .common import BASELINE_VALID_FROM, OPEN_VALID_TO, row_hash

CUSTOMER_TRACKED = ["customer_name", "email", "phone", "account_id", "gender", "dob",
                    "address", "kyc_status", "registration_date"]
BRANCH_TRACKED = ["branch_name", "location", "manager_name", "opened_date", "region",
                  "branch_type", "contact_number"]
PRODUCT_TRACKED = ["product_name", "product_type", "price", "launch_date", "is_active",
                   "category", "vendor_name"]

FACT_VALUE_COLS = ["account_id", "product_id", "branch_id", "customer_sk", "product_sk",
                   "branch_sk", "transaction_date", "timestamp", "amount", "payment_method",
                   "status", "currency", "remarks", "is_refund", "is_correction"]


def scd2_merge(current: pd.DataFrame | None, incoming: pd.DataFrame, natural_key: str,
               sk_col: str, tracked: list[str], cutoff: pd.Timestamp | None) -> tuple[pd.DataFrame, dict]:
    """Merge a clean batch into an SCD2 dimension.

    Baseline (current is None): every row becomes version 1 open from 1900-01-01.
    Incremental: an unchanged row is a no-op. A changed row closes its current version
    at `cutoff` and opens a new one. A new key is inserted as a new version.
    Returns the new dimension and a change summary (new / changed / unchanged).
    """
    inc = incoming.copy()
    inc["row_hash"] = inc.apply(lambda r: row_hash(r, tracked), axis=1)

    if current is None or current.empty:
        dim = inc.reset_index(drop=True)
        dim[sk_col] = range(1, len(dim) + 1)
        dim["valid_from"] = BASELINE_VALID_FROM
        dim["valid_to"] = OPEN_VALID_TO
        dim["is_current"] = True
        return _as_us(dim), {"new": len(dim), "changed": 0, "unchanged": 0}

    cur = current[current["is_current"]].set_index(natural_key)
    dim = current.copy()
    next_sk = int(dim[sk_col].max()) + 1
    summary = {"new": 0, "changed": 0, "unchanged": 0}
    new_rows = []
    for _, row in inc.iterrows():
        key = row[natural_key]
        if key not in cur.index:
            summary["new"] += 1
            new_rows.append(_version(row, next_sk, BASELINE_VALID_FROM if cutoff is None else cutoff))
            next_sk += 1
        elif cur.loc[key, "row_hash"] == row["row_hash"]:
            summary["unchanged"] += 1
        else:
            summary["changed"] += 1
            idx = dim.index[(dim[natural_key] == key) & dim["is_current"]][0]
            dim.loc[idx, "valid_to"] = cutoff
            dim.loc[idx, "is_current"] = False
            new_rows.append(_version(row, next_sk, cutoff))
            next_sk += 1
    if new_rows:
        dim = pd.concat([dim, pd.DataFrame(new_rows)], ignore_index=True)
    return _as_us(dim), summary


def _as_us(dim: pd.DataFrame) -> pd.DataFrame:
    """Validity columns use microseconds: 9999-12-31 does not fit in nanosecond timestamps."""
    for c in ["valid_from", "valid_to"]:
        dim[c] = dim[c].astype("datetime64[us]")
    return dim


def _version(row: pd.Series, sk: int, valid_from: pd.Timestamp) -> dict:
    out = row.drop(labels=[c for c in ["dq_flags"] if c in row.index]).to_dict()
    out["dq_flags"] = row.get("dq_flags", "")
    out.update({"valid_from": valid_from, "valid_to": OPEN_VALID_TO, "is_current": True})
    return {**out, _sk_name(out): sk}


def _sk_name(out: dict) -> str:
    # Surrogate key name follows the natural key: customer_id -> customer_sk, etc.
    for nk, sk in [("customer_id", "customer_sk"), ("branch_id", "branch_sk"), ("product_id", "product_sk")]:
        if nk in out:
            return sk
    raise KeyError("no natural key found")


def resolve_sk(facts: pd.DataFrame, dim: pd.DataFrame, fact_key: str, dim_key: str,
               sk_col: str, ts_col: str) -> pd.Series:
    """Point-in-time surrogate key: the dimension version valid at the fact timestamp.

    Falls back to the current version when no version covers the timestamp.
    Returns NaN where the natural key is not in the dimension at all (orphan).
    """
    versions = dim[[dim_key, sk_col, "valid_from", "valid_to", "is_current"]]
    merged = facts[[fact_key, ts_col]].reset_index().merge(versions, left_on=fact_key, right_on=dim_key, how="left")
    in_window = (merged[ts_col] >= merged["valid_from"]) & (merged[ts_col] < merged["valid_to"])
    merged["rank"] = in_window.astype(int) * 2 + merged["is_current"].fillna(False).astype(int)
    best = merged.sort_values(["index", "rank"], ascending=[True, False]).drop_duplicates("index")
    return best.set_index("index")[sk_col].reindex(facts.index)


def fact_exists(dim: pd.DataFrame, natural_key: str, value) -> bool:
    return bool((dim[natural_key] == value).any())


def upsert_fact(existing: pd.DataFrame | None, incoming: pd.DataFrame, batch_id: str) -> tuple[pd.DataFrame, dict]:
    """Upsert on transaction_id. Late corrections update amount/status in place, so no double counting.

    Returns the merged fact table and a change summary used by KPI 11.
    """
    inc = incoming.copy()
    inc["loaded_batch_id"] = batch_id
    if existing is None or existing.empty:
        inc["first_batch_id"] = batch_id
        return inc.reset_index(drop=True), {"inserted": len(inc), "updated": 0, "corrected": 0}

    first = existing.set_index("transaction_id")["first_batch_id"]
    inc["first_batch_id"] = inc["transaction_id"].map(first).fillna(batch_id)

    old = existing.set_index("transaction_id")
    new = inc.set_index("transaction_id")
    common = old.index.intersection(new.index)
    cmp_cols = [c for c in FACT_VALUE_COLS if c in old.columns and c in new.columns]
    changed = [t for t in common if not _same(old.loc[t, cmp_cols], new.loc[t, cmp_cols])]
    summary = {
        "inserted": len(new.index.difference(old.index)),
        "updated": len(changed),
        "corrected": int(new.loc[new.index.isin(common) & new["is_correction"].astype(bool)].shape[0]),
    }
    kept = existing[~existing["transaction_id"].isin(inc["transaction_id"])]
    merged = pd.concat([kept, inc], ignore_index=True)
    return merged, summary


def _same(a: pd.Series, b: pd.Series) -> bool:
    for x, y in zip(a.tolist(), b.tolist()):
        if pd.isna(x) and pd.isna(y):
            continue
        if x != y:
            return False
    return True
