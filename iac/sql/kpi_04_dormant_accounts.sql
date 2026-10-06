-- KPI 4: Dormant accounts = no successful INR txn in the 90 days before the as-of date,
-- including accounts that never transacted. Flag 'High-Risk Dormant' when the account's
-- latest product is a Credit Card or Loan.
-- {as_of} is replaced by the pipeline run date.
WITH last_txn AS (
    SELECT account_id, MAX(txn_ts) AS last_txn_ts
    FROM v_fact
    WHERE status = 'SUCCESS' AND currency = 'INR'
    GROUP BY account_id
),
latest_product AS (
    SELECT account_id, product_id,
           ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_ts DESC) AS rn
    FROM v_fact
    WHERE status = 'SUCCESS' AND currency = 'INR'
)
SELECT c.customer_id,
       c.account_id,
       lt.last_txn_ts,
       p.product_type,
       CASE WHEN p.product_type IN ('Credit Card', 'Loan') THEN 'High-Risk Dormant' ELSE 'Dormant' END AS dormant_flag
FROM dim_customer c
LEFT JOIN last_txn lt ON lt.account_id = c.account_id
LEFT JOIN latest_product lp ON lp.account_id = c.account_id AND lp.rn = 1
LEFT JOIN (SELECT product_id, product_type FROM dim_product WHERE is_current) p ON p.product_id = lp.product_id
WHERE c.is_current
  AND (lt.last_txn_ts IS NULL OR lt.last_txn_ts < TIMESTAMP '{as_of}' - INTERVAL '90' DAY)
ORDER BY dormant_flag, lt.last_txn_ts NULLS FIRST, c.account_id;
