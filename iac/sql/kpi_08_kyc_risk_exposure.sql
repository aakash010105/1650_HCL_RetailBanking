-- KPI 8: KYC risk exposure. Successful INR txns grouped by the customer's KYC status.
-- KYC is taken from the customer version valid at transaction time.
-- is_not_verified = kyc_status <> 'Verified'. Sum the rows where is_not_verified is true for the total.
WITH tx AS (
    SELECT f.amount,
           COALESCE(c.kyc_status, 'UNKNOWN') AS kyc_status
    FROM v_fact f
    LEFT JOIN dim_customer c ON c.customer_sk = f.customer_sk
    WHERE f.status = 'SUCCESS' AND f.currency = 'INR'
)
SELECT kyc_status,
       kyc_status <> 'Verified' AS is_not_verified,
       COUNT(*) AS txn_count,
       SUM(amount) AS total_value,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_count,
       ROUND(100.0 * SUM(amount) / SUM(SUM(amount)) OVER (), 2) AS pct_value
FROM tx
GROUP BY kyc_status
ORDER BY kyc_status;
