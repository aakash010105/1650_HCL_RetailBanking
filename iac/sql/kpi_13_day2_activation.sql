-- KPI 13 (Day 2): New account activation.
-- A) Accounts whose first loaded transaction is in the Day 2 batch (no Day 1 transaction).
-- B) Customers onboarded in Day 2 (first SCD2 version opened at the Day 2 cutoff) with zero transactions.
-- {cutoff} is replaced by the Day 2 cutoff timestamp.
WITH day1_accts AS (
    SELECT DISTINCT account_id FROM v_fact WHERE first_batch_id = 'day1-baseline'
),
day2_accts AS (
    SELECT DISTINCT account_id FROM v_fact WHERE first_batch_id = 'day2-incremental'
),
new_active AS (
    SELECT account_id FROM day2_accts
    WHERE account_id NOT IN (SELECT account_id FROM day1_accts)
),
onboarded AS (
    SELECT customer_id, MAX(account_id) AS account_id
    FROM dim_customer
    GROUP BY customer_id
    HAVING MIN(valid_from) = TIMESTAMP '{cutoff}'
)
SELECT 'FIRST_TXN_DAY2' AS activation_type,
       account_id,
       NULL AS customer_id
FROM new_active
UNION ALL
SELECT 'ONBOARDED_NO_TXN_DAY2' AS activation_type,
       o.account_id,
       o.customer_id
FROM onboarded o
WHERE o.account_id NOT IN (SELECT account_id FROM v_fact)
ORDER BY activation_type, account_id;
