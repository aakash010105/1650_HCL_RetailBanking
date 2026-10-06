"""Stage 1 (dimensions) and Stage 2 (facts) cleansing rules.

Each function takes a raw DataFrame (all text) and returns (clean_df, DqContext).
Rows that cannot be repaired are rejected with an error_code. Rows that can be
repaired are fixed and carry a dq_flags entry so the repair is auditable.
"""
from __future__ import annotations

import json
import re

import numpy as np
import pandas as pd

from .common import (
    AS_OF, DqContext, blank_to_none, close_match, dedupe, norm_email,
    norm_phone, parse_date_candidates, parse_dates, prepare, title_map,
)

GENDER_MAP = {"male": "Male", "m": "Male", "female": "Female", "f": "Female",
              "other": "Other", "unknown": "Unknown"}
KYC_ALLOWED = {"Verified", "Pending", "Rejected"}
REGION_ALLOWED = {"North", "South", "East", "West"}
BRANCH_TYPE_ALLOWED = {"Urban", "Rural"}
CATEGORY_ALLOWED = {"Retail", "Corporate", "SME"}
PRODUCT_TYPE_ALLOWED = {"Loan", "Credit Card", "Savings Account", "Fixed Deposit"}
STATUS_ALLOWED = {"SUCCESS", "FAILED", "PENDING"}
PAYMENT_ALLOWED = {"UPI": "UPI", "netbanking": "NetBanking", "cash": "Cash",
                   "card": "Card", "cheque": "Cheque"}
REFUND_TRUE = {"true", "y", "1", "yes"}
REFUND_FALSE = {"false", "n", "0", "no"}

CUSTOMER_BUSINESS_COLS = ["customer_name", "email", "phone", "account_id", "gender", "dob",
                          "address", "kyc_status", "registration_date"]
BRANCH_BUSINESS_COLS = ["branch_name", "location", "manager_name", "opened_date", "region",
                        "branch_type", "contact_number"]
PRODUCT_BUSINESS_COLS = ["product_name", "product_type", "price", "launch_date", "is_active",
                         "category", "vendor_name"]
TXN_BUSINESS_COLS = ["account_id", "product_id", "branch_id", "transaction_date", "timestamp",
                     "amount", "payment_method", "status", "currency", "remarks", "is_refund"]

CORRECTION_MARKER = "[late correction]"


# ---------------------------------------------------------------- customers
def cleanse_customers(raw: pd.DataFrame, batch_id: str, source_file: str):
    ctx = DqContext("customers", batch_id, source_file)
    df = prepare(raw, source_file)
    # transaction_id is a per-row transaction reference, not a customer attribute.
    df = df.drop(columns=[c for c in ["transaction_id"] if c in df.columns])
    df = blank_to_none(df, ["customer_id", "customer_name", "email", "phone", "account_id",
                            "gender", "dob", "address", "kyc_status", "registration_date"])
    df["customer_id"] = df["customer_id"].map(lambda v: v.upper() if v else v)
    df["account_id"] = df["account_id"].map(lambda v: v.upper() if v else v)

    df = ctx.reject(df, df["customer_id"].isna() | df["account_id"].isna() | df["customer_name"].isna(),
                    "CU-STRUCT", "MALFORMED_STRUCTURE", "customer_id, account_id, or customer_name missing")

    # Email: fix casing and '@@'; anything still invalid is nulled and flagged.
    raw_email = df["email"]
    df["email"] = df["email"].map(norm_email)
    bad_email = df["email"].isna()
    ctx.flag(df, raw_email.isna(), "EMAIL_MISSING")
    ctx.flag(df, bad_email & raw_email.notna(), "EMAIL_INVALID")

    # Phone: normalize to +91-XXXXXXXXXX; unparseable values are nulled and flagged.
    raw_phone = df["phone"]
    df["phone"] = df["phone"].map(norm_phone)
    ctx.flag(df, raw_phone.isna(), "PHONE_MISSING")
    ctx.flag(df, df["phone"].isna() & raw_phone.notna(), "PHONE_INVALID")

    df["gender"] = df["gender"].map(lambda v: GENDER_MAP.get(v.lower(), "Unknown") if v else "Unknown")
    df["kyc_status"] = df["kyc_status"].map(lambda v: title_map(v, KYC_ALLOWED))
    df = ctx.reject(df, df["kyc_status"].isna(), "CU-KYC", "INVALID_KYC_STATUS", "kyc_status not in Verified/Pending/Rejected")

    # DOB: blank is flagged; future or under-18 dates are unfixable and quarantined.
    dob = parse_dates(df["dob"])
    ctx.flag(df, df["dob"].isna(), "DOB_MISSING")
    age = (AS_OF - dob).dt.days / 365.25
    df = ctx.reject(df, dob.notna() & ((dob > AS_OF) | (age < 18) | (age > 110)),
                    "CU-DOB", "INVALID_DOB", "dob in the future, under 18, or over 110 years")
    df["dob"] = dob[df.index]

    # Registration date: several formats. Day-first is assumed for ambiguous slash/dash dates.
    reg = []
    ambiguous = []
    for v in df["registration_date"]:
        cands = parse_date_candidates(v)
        reg.append(cands[0] if cands else pd.NaT)
        ambiguous.append(len(cands) > 1)
    df["registration_date"] = pd.Series(reg, index=df.index, dtype="datetime64[ns]")
    ctx.flag(df, pd.Series(ambiguous, index=df.index), "REG_DATE_AMBIGUOUS_ASSUMED_DMY")
    df = ctx.reject(df, df["registration_date"].isna(), "CU-REGDATE", "INVALID_DATE", "registration_date unparseable")
    df = ctx.reject(df, df["registration_date"] > AS_OF, "CU-REGDATE-FUT", "FUTURE_DATE", "registration_date after run date")
    df["registration_date"] = pd.to_datetime(df["registration_date"])

    df = dedupe(ctx, df, "customer_id", CUSTOMER_BUSINESS_COLS, "CU")

    # Account keys must map to one customer. When two customers share an account,
    # keep the one whose numeric id matches the account (C046 <-> A0046); reject the rest.
    dup_acct = df["account_id"].duplicated(keep=False)
    if dup_acct.any():
        def num(v):
            m = re.search(r"\d+", v)
            return int(m.group()) if m else None
        match = df.apply(lambda r: num(r["customer_id"]) == num(r["account_id"]), axis=1)
        # Keep the copy whose id matches its account; reject the rest. If the group
        # does not have exactly one matching customer, reject the whole group.
        reject_mask = dup_acct & ~match
        for _, grp in df[dup_acct].groupby("account_id"):
            if match[grp.index].sum() != 1:
                reject_mask[grp.index] = True
        df = ctx.reject(df, reject_mask, "CU-ACCT", "ACCOUNT_CONFLICT",
                        "account_id shared with another customer; not the numeric match")

    df["dob"] = df["dob"].where(df["dob"].notna(), None)
    return df.reset_index(drop=True), ctx


def clean_out_customers(df: pd.DataFrame) -> pd.DataFrame:
    cols = ["customer_id", "customer_name", "email", "phone", "account_id", "gender", "dob",
            "address", "kyc_status", "registration_date", "dq_flags"]
    return df[cols].reset_index(drop=True)


# ---------------------------------------------------------------- branches
def cleanse_branches(raw: pd.DataFrame, batch_id: str, source_file: str):
    ctx = DqContext("branches", batch_id, source_file)
    df = prepare(raw, source_file)
    df = blank_to_none(df, ["branch_id", "branch_name", "location", "manager_name", "opened_date",
                            "region", "branch_type", "contact_number"])
    df["branch_id"] = df["branch_id"].map(lambda v: v.upper() if v else v)
    df = ctx.reject(df, df["branch_id"].isna() | df["branch_name"].isna(),
                    "BR-STRUCT", "MALFORMED_STRUCTURE", "branch_id or branch_name missing")

    df["region"] = df["region"].map(lambda v: title_map(v, REGION_ALLOWED))
    df = ctx.reject(df, df["region"].isna(), "BR-REGION", "INVALID_REGION", "region not North/South/East/West")
    df["branch_type"] = df["branch_type"].map(lambda v: title_map(v, BRANCH_TYPE_ALLOWED))
    df = ctx.reject(df, df["branch_type"].isna(), "BR-TYPE", "INVALID_BRANCH_TYPE", "branch_type not Urban/Rural")

    # Manager: strip the "(New)" label from re-sent rows; fill missing with UNASSIGNED and flag.
    label = df["manager_name"].fillna("").str.contains(r"\(New\)$", regex=True)
    df.loc[label, "manager_name"] = df.loc[label, "manager_name"].str.replace(r"\s*\(New\)$", "", regex=True)
    ctx.flag(df, label, "MANAGER_LABEL_STRIPPED")
    ctx.flag(df, df["manager_name"].isna(), "MANAGER_MISSING")
    df["manager_name"] = df["manager_name"].fillna("UNASSIGNED")

    raw_phone = df["contact_number"]
    df["contact_number"] = df["contact_number"].map(norm_phone)
    ctx.flag(df, df["contact_number"].isna() & raw_phone.notna(), "PHONE_INVALID")

    df["opened_date"] = parse_dates(df["opened_date"])
    df = ctx.reject(df, df["opened_date"].isna() | (df["opened_date"] > AS_OF), "BR-OPENED", "INVALID_DATE",
                    "opened_date missing, unparseable, or future")

    df = dedupe(ctx, df, "branch_id", BRANCH_BUSINESS_COLS, "BR")
    return df.reset_index(drop=True), ctx


def clean_out_branches(df: pd.DataFrame) -> pd.DataFrame:
    cols = ["branch_id", "branch_name", "location", "manager_name", "opened_date", "region",
            "branch_type", "contact_number", "dq_flags"]
    return df[cols].reset_index(drop=True)


# ---------------------------------------------------------------- products
def load_products(path: str) -> tuple[list[dict], bool]:
    """Parse products JSON, repairing the missing-comma defect between objects if needed.

    Returns (records, repaired). An unrepairable file returns ([], False) and the caller
    rejects the whole file.
    """
    text = open(path, encoding="utf-8").read()
    try:
        return json.loads(text), False
    except json.JSONDecodeError:
        pass
    repaired_text = re.sub(r"\}\s*\n(\s*)\{", r"},\n\1{", text)
    try:
        return json.loads(repaired_text), True
    except json.JSONDecodeError:
        return [], False


def cleanse_products(records: list[dict], batch_id: str, source_file: str, repaired: bool):
    ctx = DqContext("products", batch_id, source_file)
    raw = pd.DataFrame(records).astype(object)
    if raw.empty:
        return raw, ctx
    raw.insert(0, "_src_row", np.arange(len(raw)) + 1)  # JSON array index, 1-based
    raw["_raw"] = [json.dumps(r, default=str) for r in records]
    raw["dq_flags"] = ""  # file-level repair is recorded in the batch audit, not per row

    df = raw
    df["product_id"] = df["product_id"].map(lambda v: str(v).strip().upper() if v is not None else None)
    df = ctx.reject(df, df["product_id"].isna() | (df["product_id"] == ""),
                    "PR-STRUCT", "MALFORMED_STRUCTURE", "product_id missing")

    def to_number(v):
        if isinstance(v, bool) or v is None:
            return None
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).replace(",", "").replace("Rs.", "").strip()
        try:
            return float(s)
        except ValueError:
            return None

    df["price"] = df["price"].map(to_number)
    df = ctx.reject(df, df["price"].isna(), "PR-PRICE-NUM", "INVALID_PRICE", "price not numeric")
    df = ctx.reject(df, df["price"] < 0, "PR-PRICE-NEG", "NEGATIVE_PRICE", "price below zero; rejected, not auto-corrected")
    df["price"] = df["price"].astype("int64")

    df["category"] = df["category"].map(lambda v: title_map(str(v).strip(), CATEGORY_ALLOWED) if v else None)
    df = ctx.reject(df, df["category"].isna(), "PR-CAT", "INVALID_CATEGORY", "category not Retail/Corporate/SME")
    df["product_type"] = df["product_type"].map(lambda v: title_map(str(v).strip(), PRODUCT_TYPE_ALLOWED) if v else None)
    df = ctx.reject(df, df["product_type"].isna(), "PR-TYPE", "INVALID_PRODUCT_TYPE", "product_type not recognized")

    # is_active: booleans, "true"/"false" strings, and missing/null. Missing defaults to False.
    def to_bool(v):
        if isinstance(v, bool):
            return v, False
        if isinstance(v, str) and v.strip().lower() in REFUND_TRUE:
            return True, False
        if isinstance(v, str) and v.strip().lower() in REFUND_FALSE:
            return False, False
        return False, True  # None, NaN, unrecognized: default to inactive and flag

    conv = df["is_active"].map(to_bool)
    df["is_active"] = conv.map(lambda t: t[0]).astype(bool)
    ctx.flag(df, conv.map(lambda t: t[1]), "IS_ACTIVE_DEFAULTED")

    df["vendor_name"] = df["vendor_name"].map(lambda v: str(v).strip() if v else None)
    df["product_name"] = df["product_name"].map(lambda v: str(v).strip() if v else None)

    df["launch_date"] = parse_dates(df["launch_date"])
    df = ctx.reject(df, df["launch_date"].isna() | (df["launch_date"] > AS_OF), "PR-LAUNCH", "INVALID_DATE",
                    "launch_date missing, unparseable, or future")

    df = dedupe(ctx, df, "product_id", PRODUCT_BUSINESS_COLS, "PR")
    return df.reset_index(drop=True), ctx


def clean_out_products(df: pd.DataFrame) -> pd.DataFrame:
    cols = ["product_id", "product_name", "product_type", "price", "launch_date", "is_active",
            "category", "vendor_name", "dq_flags"]
    return df[cols].reset_index(drop=True)


# ---------------------------------------------------------------- transactions
def _to_amount(v):
    if v is None:
        return None
    s = str(v).replace(",", "").replace("Rs.", "").strip()
    try:
        return float(s)
    except ValueError:
        return None


def cleanse_transactions(raw: pd.DataFrame, batch_id: str, source_file: str):
    """Stage 2 cleansing, without the referential checks (those run in the caller)."""
    ctx = DqContext("transactions", batch_id, source_file)
    df = prepare(raw, source_file)
    df = blank_to_none(df, ["transaction_id", "account_id", "product_id", "branch_id", "date",
                            "timestamp", "amount", "payment_method", "status", "currency",
                            "remarks", "is_refund"])
    for c in ["transaction_id", "account_id", "product_id", "branch_id"]:
        df[c] = df[c].map(lambda v: v.upper() if v else v)

    df = ctx.reject(df, df[["transaction_id", "account_id", "product_id", "branch_id", "timestamp"]].isna().any(axis=1),
                    "TX-STRUCT", "MALFORMED_STRUCTURE", "required key or timestamp missing")

    # Amount: strip currency symbols and commas. Blank, zero, and negative are rejected.
    df["amount"] = df["amount"].map(_to_amount)
    df = ctx.reject(df, df["amount"].isna(), "TX-AMT-BLANK", "AMOUNT_BLANK", "amount blank or not numeric")
    df = ctx.reject(df, df["amount"] == 0, "TX-AMT-ZERO", "AMOUNT_ZERO", "amount is zero")
    df = ctx.reject(df, df["amount"] < 0, "TX-AMT-NEG", "AMOUNT_NEGATIVE", "amount is negative")
    df["amount"] = df["amount"].astype("int64")

    # Timestamp is authoritative. The date column is reconciled to it, and any disagreement is logged.
    ts = pd.to_datetime(df["timestamp"], format="%Y-%m-%d %H:%M:%S", errors="coerce")
    df = ctx.reject(df, ts.isna(), "TX-TS", "INVALID_DATE", "timestamp unparseable")
    df["timestamp"] = ts
    df = ctx.reject(df, ts >= AS_OF + pd.Timedelta(days=1), "TX-FUT", "FUTURE_DATE",
                    "timestamp after run date")
    ts = df["timestamp"]

    txn_date = []
    mismatch = []
    ambiguous = []
    future_date = []
    for raw_date, t in zip(df["date"], ts):
        cands = parse_date_candidates(raw_date)
        chosen = next((c for c in cands if c == t.normalize()), None)
        txn_date.append(chosen if chosen is not None else t.normalize())
        mismatch.append(chosen is None and bool(cands))
        ambiguous.append(len(cands) > 1)
        future_date.append(bool(cands) and min(cands) > AS_OF)
    df["transaction_date"] = pd.to_datetime(txn_date)

    ctx.flag(df, pd.Series(mismatch, index=df.index), "DATE_RECONCILED_TO_TIMESTAMP")
    ctx.flag(df, pd.Series(ambiguous, index=df.index) & ~pd.Series(mismatch, index=df.index), "DATE_AMBIGUOUS_RESOLVED_BY_TS")
    # A future date column is quarantined even when the timestamp is valid. The date cannot be trusted.
    df = ctx.reject(df, pd.Series(future_date, index=df.index), "TX-FUT-DATE", "FUTURE_DATE",
                    "date column after run date")

    df["currency"] = df["currency"].map(lambda v: v.upper() if v else "INR")
    ctx.flag(df, df["currency"] != "INR", "NON_INR_CURRENCY")

    df["status"] = df["status"].map(lambda v: close_match(v, STATUS_ALLOWED) if v else None)
    df = ctx.reject(df, df["status"].isna(), "TX-STATUS", "INVALID_STATUS", "status not Success/Failed/Pending")

    df["payment_method"] = df["payment_method"].map(lambda v: next((canon for k, canon in PAYMENT_ALLOWED.items()
                                                                  if k.lower() == v.lower()), None) if v else None)
    df = ctx.reject(df, df["payment_method"].isna(), "TX-PAY", "INVALID_PAYMENT_METHOD", "payment_method not recognized")

    refund_raw = df["is_refund"]
    def to_refund(v):
        if v is None:
            return False, True
        s = str(v).strip().lower()
        if s in REFUND_TRUE:
            return True, False
        if s in REFUND_FALSE:
            return False, False
        return False, True
    conv = refund_raw.map(to_refund)
    df["is_refund"] = conv.map(lambda t: t[0]).astype(bool)
    ctx.flag(df, conv.map(lambda t: t[1]), "IS_REFUND_DEFAULTED")

    # Late corrections carry a remarks marker. Strip it, flag the row, and let the fact MERGE apply it.
    is_corr = df["remarks"].fillna("").str.contains(re.escape(CORRECTION_MARKER))
    df["is_correction"] = is_corr.astype(bool)
    df["remarks"] = df["remarks"].fillna("").str.replace(CORRECTION_MARKER, "", regex=False).str.strip()
    df["remarks"] = df["remarks"].map(lambda v: v if v else None)
    ctx.flag(df, is_corr, "LATE_CORRECTION")

    df = dedupe(ctx, df, "transaction_id", TXN_BUSINESS_COLS, "TX")
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df.reset_index(drop=True), ctx


def clean_out_transactions(df: pd.DataFrame) -> pd.DataFrame:
    cols = ["transaction_id", "account_id", "product_id", "branch_id", "transaction_date",
            "timestamp", "amount", "payment_method", "status", "currency", "remarks",
            "is_refund", "is_correction", "dq_flags"]
    return df[cols].reset_index(drop=True)
