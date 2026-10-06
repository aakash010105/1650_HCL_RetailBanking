"""Shared helpers: reject/flag collection, normalizers, dedup, and date parsing.

Every rule records a rule_id and an error_code. Rejected rows keep their raw
payload so they can be reprocessed later.
"""
from __future__ import annotations

import difflib
import json
import re
from datetime import datetime

import numpy as np
import pandas as pd

# Pipeline "today". Future-date rules compare against this, not wall-clock time.
AS_OF = pd.Timestamp("2026-10-06")
BASELINE_VALID_FROM = pd.Timestamp("1900-01-01")
OPEN_VALID_TO = pd.Timestamp("9999-12-31")

REJECT_COLUMNS = [
    "entity", "batch_id", "source_file", "source_row",
    "rule_id", "error_code", "error_detail", "raw_payload",
]


def write_parquet(df: pd.DataFrame, path) -> None:
    """Write Parquet with microsecond timestamps. Athena cannot read nanosecond timestamps."""
    from pathlib import Path
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)


def _add_flag(existing: str, code: str) -> str:
    return code if not existing else f"{existing};{code}"


class DqContext:
    """Collects rejects and flags for one entity in one batch."""

    def __init__(self, entity: str, batch_id: str, source_file: str):
        self.entity = entity
        self.batch_id = batch_id
        self.source_file = source_file
        self.rejects: list[pd.DataFrame] = []

    def reject(self, df: pd.DataFrame, mask: pd.Series, rule_id: str, code: str, detail: str) -> pd.DataFrame:
        """Move rows where mask is True into the reject list and return the rest."""
        bad = df[mask]
        if bad.empty:
            return df
        self.rejects.append(pd.DataFrame({
            "entity": self.entity,
            "batch_id": self.batch_id,
            "source_file": self.source_file,
            "source_row": bad["_src_row"].to_numpy(),
            "rule_id": rule_id,
            "error_code": code,
            "error_detail": detail,
            "raw_payload": bad["_raw"].to_numpy(),
        }))
        return df[~mask].copy()

    def flag(self, df: pd.DataFrame, mask: pd.Series, code: str) -> None:
        """Mark rows that were repaired or are suspicious but still load."""
        if mask.any():
            df.loc[mask, "dq_flags"] = [_add_flag(f, code) for f in df.loc[mask, "dq_flags"]]

    def rejects_frame(self) -> pd.DataFrame:
        if not self.rejects:
            return pd.DataFrame(columns=REJECT_COLUMNS)
        return pd.concat(self.rejects, ignore_index=True)


def prepare(df: pd.DataFrame, source_file: str) -> pd.DataFrame:
    """Read every column as text, keep the source line number and raw JSON payload."""
    df = df.copy()
    df["_src_row"] = np.arange(len(df)) + 2  # header is line 1
    raw_cols = [c for c in df.columns if not c.startswith("_")]
    df["_raw"] = [json.dumps(r, default=str) for r in df[raw_cols].to_dict("records")]
    df["dq_flags"] = ""
    return df


def blank_to_none(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Strip whitespace; empty strings become None so they are treated as missing."""
    for c in cols:
        df[c] = df[c].astype("object").map(lambda v: None if v is None or (isinstance(v, str) and v.strip() == "") else (v.strip() if isinstance(v, str) else v))
    return df


def title_map(value: str | None, allowed: set[str]) -> str | None:
    """Case-insensitive match against an allowed set; returns the canonical spelling or None."""
    if value is None:
        return None
    lookup = {a.lower(): a for a in allowed}
    return lookup.get(value.strip().lower())


def close_match(value: str | None, allowed: set[str], cutoff: float = 0.75) -> str | None:
    """Canonicalize typos such as 'Succes' or 'Faild' against an allowed set."""
    if value is None:
        return None
    exact = title_map(value, allowed)
    if exact:
        return exact
    hit = difflib.get_close_matches(value.upper(), [a.upper() for a in allowed], n=1, cutoff=cutoff)
    if not hit:
        return None
    return next(a for a in allowed if a.upper() == hit[0])


def norm_phone(value: str | None) -> str | None:
    """Return +91-XXXXXXXXXX for a valid Indian 10-digit number, else None."""
    if value is None:
        return None
    v = value.strip()
    if v.startswith("+91"):
        body = v[3:]
    else:
        digits = re.sub(r"\D", "", v)
        body = digits[2:] if len(digits) == 12 and digits.startswith("91") else v
    digits = re.sub(r"\D", "", body)
    return f"+91-{digits}" if len(digits) == 10 else None


EMAIL_RE = re.compile(r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$")


def norm_email(value: str | None) -> str | None:
    if value is None:
        return None
    v = value.strip().lower().replace("@@", "@")
    return v if EMAIL_RE.match(v) else None


def parse_date_candidates(value: str | None) -> list[pd.Timestamp]:
    """All valid readings of a date string.

    ISO (YYYY-MM-DD, YYYY/MM/DD) has one reading. Slash/dash day-first and
    month-first forms (DD/MM/YYYY, MM/DD/YYYY) can have two. Values with a
    part greater than 12 have exactly one reading.
    """
    if value is None:
        return []
    v = value.strip()
    readings: list[tuple[int, int, int]] = []
    m = re.fullmatch(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})", v)
    if m:
        y, mo, d = map(int, m.groups())
        readings = [(y, mo, d)]
    else:
        m = re.fullmatch(r"(\d{1,2})[-/](\d{1,2})[-/](\d{4})", v)
        if m:
            a, b, y = map(int, m.groups())
            if a > 12 and b <= 12:
                readings = [(y, b, a)]
            elif b > 12 and a <= 12:
                readings = [(y, a, b)]
            else:
                readings = [(y, b, a), (y, a, b)]  # day-first, then month-first
    out: list[pd.Timestamp] = []
    for y, mo, d in readings:
        try:
            ts = pd.Timestamp(datetime(y, mo, d))
        except ValueError:
            continue
        if ts not in out:
            out.append(ts)
    return out


def first_date(value: str | None) -> pd.Timestamp:
    """Single-reading date parse (day-first when ambiguous). NaT when unparseable."""
    cands = parse_date_candidates(value)
    return cands[0] if cands else pd.NaT


def parse_dates(series: pd.Series) -> pd.Series:
    """Map a text column to datetime64[ns]. Stays datetime-typed even when the column is empty."""
    return pd.Series([first_date(v) for v in series], index=series.index, dtype="datetime64[ns]")


def row_hash(row: pd.Series, cols: list[str]) -> str:
    import hashlib
    payload = "|".join("" if pd.isna(row[c]) else str(row[c]) for c in cols)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def dedupe(ctx: DqContext, df: pd.DataFrame, key: str, business_cols: list[str], rule_prefix: str) -> pd.DataFrame:
    """Dedup on the business key.

    1. Exact duplicates (all business columns equal) keep the first copy.
    2. Near duplicates (same non-null values, differing only in missing values)
       keep the copy with the fewest DQ flags, ties go to the first occurrence.
    3. Conflicting duplicates (non-null values differ) are all quarantined,
       because the pipeline cannot choose the correct value.
    """
    df = df.copy()
    df["_flag_count"] = df["dq_flags"].map(lambda f: 0 if not f else f.count(";") + 1)
    cmp_cols = [key] + business_cols
    eq = df[cmp_cols].astype(str)
    exact_dup = eq.duplicated(keep="first")
    df = ctx.reject(df, exact_dup, f"{rule_prefix}-DUP", "DUPLICATE_RECORD", "exact duplicate of an earlier row")

    dup_keys = df[df[key].duplicated(keep=False)][key].unique()
    if len(dup_keys) == 0:
        return df.drop(columns="_flag_count")

    conflict_keys = []
    for k in dup_keys:
        grp = df[df[key] == k]
        for c in business_cols:
            if grp[c].dropna().astype(str).nunique() > 1:
                conflict_keys.append(k)
                break
    conflict_mask = df[key].isin(conflict_keys)
    df = ctx.reject(df, conflict_mask, f"{rule_prefix}-CONFLICT", "CONFLICTING_DUPLICATE",
                    "same business key with different non-null values")

    near_mask = df[key].duplicated(keep=False)
    if near_mask.any():
        ordered = df.reset_index().sort_values(["_flag_count", "index"])
        keep_idx = ordered.drop_duplicates(key, keep="first")["index"]
        drop_mask = near_mask & ~df.index.isin(keep_idx)
        df = ctx.reject(df, drop_mask, f"{rule_prefix}-DUP", "DUPLICATE_RECORD",
                        "near duplicate, lower-quality copy removed")
    return df.drop(columns="_flag_count")
