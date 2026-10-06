"""Batch definitions and thresholds. Source filenames match the drop in Retailbank_SourceData/."""

BATCHES = {
    "day1": {
        "batch_id": "day1-baseline",
        "load_type": "baseline",
        "dt": "2026-10-01",
        "cutoff_ts": "2026-10-01T00:00:00",
        "files": {
            "branches": "branches.csv",
            "products": "products_json.txt",
            "customers": "customers.csv",
            "transactions": "transactions.csv",
        },
    },
    "day2": {
        "batch_id": "day2-incremental",
        "load_type": "incremental",
        "dt": "2026-10-02",
        "cutoff_ts": "2026-10-02T00:00:00",
        "files": {
            "branches": "branches_2.csv",
            "products": "products_2_json.txt",
            "customers": "customer_updates_2.csv",
            "transactions": "transactions_2.csv",
        },
    },
}

# Max data-quality reject rate per entity before the batch fails. MISSING_DIM is excluded.
# Placeholder values: confirm with the data owners.
QUARANTINE_THRESHOLD = {"default": 0.25}
