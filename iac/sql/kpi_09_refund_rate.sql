-- KPI 9: Refund rate by product and branch. Denominator is all successful INR txns for that product and branch.
-- Shows refund count % and refund value % of total.
SELECT p.product_id,
       p.product_name,
       f.branch_id,
       COUNT(*) AS total_count,
       SUM(CASE WHEN f.is_refund THEN 1 ELSE 0 END) AS refund_count,
       ROUND(100.0 * SUM(CASE WHEN f.is_refund THEN 1 ELSE 0 END) / COUNT(*), 2) AS refund_count_pct,
       SUM(f.amount) AS total_value,
       SUM(CASE WHEN f.is_refund THEN f.amount ELSE 0 END) AS refund_value,
       ROUND(100.0 * SUM(CASE WHEN f.is_refund THEN f.amount ELSE 0 END) / NULLIF(SUM(f.amount), 0), 2) AS refund_value_pct
FROM v_fact f
LEFT JOIN (SELECT product_id, product_name FROM dim_product WHERE is_current) p ON p.product_id = f.product_id
WHERE f.status = 'SUCCESS' AND f.currency = 'INR'
GROUP BY 1, 2, 3
ORDER BY refund_value_pct DESC NULLS LAST, p.product_id, f.branch_id;
