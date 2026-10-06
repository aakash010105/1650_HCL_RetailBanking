-- KPI 7: Branch performance ranking within region, on net successful INR volume (full history).
-- Region is the branch's current region (ranks the current structure). RANK and DENSE_RANK are both shown.
-- TOP = rank 1 in region, BOTTOM = last place in region.
WITH perf AS (
    SELECT b.region,
           f.branch_id,
           b.branch_name,
           SUM(CASE WHEN f.is_refund THEN -f.amount ELSE f.amount END) AS net_volume,
           COUNT(*) AS txn_count
    FROM v_fact f
    JOIN dim_branch b ON b.branch_id = f.branch_id AND b.is_current
    WHERE f.status = 'SUCCESS' AND f.currency = 'INR'
    GROUP BY 1, 2, 3
),
ranked AS (
    SELECT *,
           RANK() OVER (PARTITION BY region ORDER BY net_volume DESC) AS rnk,
           DENSE_RANK() OVER (PARTITION BY region ORDER BY net_volume DESC) AS dense_rnk,
           RANK() OVER (PARTITION BY region ORDER BY net_volume ASC) AS rnk_asc
    FROM perf
)
SELECT region,
       branch_id,
       branch_name,
       net_volume,
       txn_count,
       rnk,
       dense_rnk,
       CASE WHEN rnk = 1 THEN 'TOP' WHEN rnk_asc = 1 THEN 'BOTTOM' END AS performer_flag
FROM ranked
ORDER BY region, rnk, branch_id;
