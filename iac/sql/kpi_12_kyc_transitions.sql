-- KPI 12 (Day 2): Customers whose KYC status changed between SCD2 versions.
-- The closed version's valid_to equals the new version's valid_from (the batch cutoff).
SELECT old.customer_id,
       old.customer_name,
       old.kyc_status AS from_status,
       new.kyc_status AS to_status,
       new.valid_from AS changed_on
FROM dim_customer old
JOIN dim_customer new
  ON new.customer_id = old.customer_id
 AND new.valid_from = old.valid_to
WHERE old.kyc_status <> new.kyc_status
ORDER BY changed_on, old.customer_id;
