-- ============================================================
-- ROOT CAUSE INVESTIGATION: Transaction vs Settlement Date Gap
-- Use Case: Identify incentive/payout timing discrepancies
--           that cause compliance and reporting errors
-- Author: Anna Rupa Anthony
-- ============================================================


-- STEP 1: Identify records where settlement date lags beyond acceptable threshold
SELECT
    t.transaction_id,
    t.partner_id,
    t.program_code,
    t.transaction_date,
    t.settlement_date,
    DATEDIFF(DAY, t.transaction_date, t.settlement_date) AS days_gap,
    t.incentive_amount,
    t.status
FROM transactions t
WHERE
    t.status = 'PENDING'
    AND DATEDIFF(DAY, t.transaction_date, t.settlement_date) > 5  -- threshold breach
    AND t.transaction_date >= DATEADD(MONTH, -3, GETDATE())
ORDER BY days_gap DESC;


-- STEP 2: Aggregate gap analysis by program to identify systemic issues
SELECT
    program_code,
    COUNT(*)                                         AS total_transactions,
    COUNT(CASE WHEN DATEDIFF(DAY, transaction_date, settlement_date) > 5 THEN 1 END)
                                                     AS breach_count,
    ROUND(
        100.0 * COUNT(CASE WHEN DATEDIFF(DAY, transaction_date, settlement_date) > 5 THEN 1 END)
        / COUNT(*), 2
    )                                                AS breach_pct,
    AVG(DATEDIFF(DAY, transaction_date, settlement_date)) AS avg_gap_days,
    MAX(DATEDIFF(DAY, transaction_date, settlement_date)) AS max_gap_days
FROM transactions
WHERE transaction_date >= DATEADD(MONTH, -3, GETDATE())
GROUP BY program_code
ORDER BY breach_pct DESC;


-- STEP 3: Cross-reference with partner eligibility to rule out config issues
SELECT
    t.transaction_id,
    t.partner_id,
    p.partner_name,
    p.eligibility_start_date,
    p.eligibility_end_date,
    t.transaction_date,
    CASE
        WHEN t.transaction_date < p.eligibility_start_date THEN 'BEFORE_ELIGIBILITY'
        WHEN t.transaction_date > p.eligibility_end_date   THEN 'AFTER_ELIGIBILITY'
        ELSE 'IN_SCOPE'
    END AS eligibility_status
FROM transactions t
JOIN partners p ON t.partner_id = p.partner_id
WHERE t.status = 'PENDING'
  AND t.transaction_date >= DATEADD(MONTH, -3, GETDATE());


-- STEP 4: Trace the issue to upstream data load — check EDL load timestamps
-- Helps determine if delay is in source data arrival vs processing logic
SELECT
    edl.load_id,
    edl.source_system,
    edl.load_timestamp,
    edl.record_count,
    edl.status        AS load_status,
    DATEDIFF(MINUTE, edl.expected_load_time, edl.load_timestamp) AS load_delay_minutes
FROM edl_load_log edl
WHERE edl.load_timestamp >= DATEADD(DAY, -7, GETDATE())
  AND edl.source_system IN ('INCENTIVES', 'SETTLEMENTS')
ORDER BY edl.load_timestamp DESC;
