-- KPI 2: Monthly successful INR volume per branch and region, with month-over-month %.
-- Region is taken from the branch version valid at transaction time (point-in-time).
WITH monthly AS (
    SELECT b.region,
           f.branch_id,
           date_trunc('month', f.transaction_date) AS month,
           SUM(f.amount) AS volume,
           COUNT(*) AS txn_count
    FROM v_fact f
    JOIN dim_branch b ON b.branch_sk = f.branch_sk
    WHERE f.status = 'SUCCESS' AND f.currency = 'INR'
    GROUP BY 1, 2, 3
)
SELECT region,
       branch_id,
       month,
       volume,
       txn_count,
       LAG(volume) OVER w AS prev_month_volume,
       ROUND(100.0 * (volume - LAG(volume) OVER w) / NULLIF(LAG(volume) OVER w, 0), 2) AS mom_pct
FROM monthly
WINDOW w AS (PARTITION BY branch_id ORDER BY month)
ORDER BY region, branch_id, month;
