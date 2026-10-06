-- KPI 1: Top 5 customers by net successful INR volume (refunds subtracted).
-- Ties: higher txn count first, then account_id. Customer identity from the current dim row.
WITH net AS (
    SELECT account_id,
           SUM(CASE WHEN is_refund THEN -amount ELSE amount END) AS net_volume,
           COUNT(*) AS txn_count
    FROM v_fact
    WHERE status = 'SUCCESS' AND currency = 'INR'
    GROUP BY account_id
)
SELECT ROW_NUMBER() OVER (ORDER BY n.net_volume DESC, n.txn_count DESC, n.account_id) AS rnk,
       c.customer_id,
       c.customer_name,
       n.account_id,
       n.net_volume,
       n.txn_count
FROM net n
LEFT JOIN dim_customer c ON c.account_id = n.account_id AND c.is_current
ORDER BY rnk
LIMIT 5;
