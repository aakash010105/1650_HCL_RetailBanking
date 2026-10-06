-- KPI 5: Suspicious transaction monitoring. All statuses, INR only (monitoring, not financial reporting).
--   R1 HIGH_VALUE         amount > 100000
--   R2 VELOCITY_3_IN_10M  3 or more txns from the same account in any 10-minute window (incl. current)
--   R3 NIGHT_00_05        txn timestamp between 00:00 and 04:59
WITH base AS (
    SELECT transaction_id, account_id, txn_ts, amount, status, branch_id
    FROM v_fact
    WHERE currency = 'INR'
),
scored AS (
    SELECT *,
           COUNT(*) OVER (PARTITION BY account_id ORDER BY txn_ts
                          RANGE BETWEEN INTERVAL '10' MINUTE PRECEDING AND CURRENT ROW) AS txns_last_10m
    FROM base
)
SELECT transaction_id,
       account_id,
       branch_id,
       txn_ts,
       amount,
       status,
       txns_last_10m,
       concat_ws('; ',
                 CASE WHEN amount > 100000 THEN 'HIGH_VALUE' END,
                 CASE WHEN txns_last_10m >= 3 THEN 'VELOCITY_3_IN_10M' END,
                 CASE WHEN hour(txn_ts) < 5 THEN 'NIGHT_00_05' END) AS risk_reason
FROM scored
WHERE amount > 100000
   OR txns_last_10m >= 3
   OR hour(txn_ts) < 5
ORDER BY txn_ts, transaction_id;
