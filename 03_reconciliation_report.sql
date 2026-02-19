-- ============================================================
-- END-TO-END RECONCILIATION REPORT
-- Use Case: Reconcile incentive records between source system
--           (EDL) and target reporting system to identify gaps,
--           mismatches, and missing records before month-end close
-- Author: Anna Rupa Anthony
-- ============================================================


-- -------------------------------------------------------
-- STEP 1: Summary reconciliation — counts and amounts
-- -------------------------------------------------------
WITH source_summary AS (
    SELECT
        program_code,
        COUNT(*)                AS source_count,
        SUM(incentive_amount)   AS source_total
    FROM edl_transactions           -- source: Enterprise Data Lake
    WHERE load_date = CAST(GETDATE() AS DATE)
    GROUP BY program_code
),
target_summary AS (
    SELECT
        program_code,
        COUNT(*)                AS target_count,
        SUM(incentive_amount)   AS target_total
    FROM reporting_transactions     -- target: reporting system
    WHERE report_date = CAST(GETDATE() AS DATE)
    GROUP BY program_code
)
SELECT
    COALESCE(s.program_code, t.program_code)    AS program_code,
    s.source_count,
    t.target_count,
    s.source_count - t.target_count             AS count_variance,
    ROUND(s.source_total, 2)                    AS source_total,
    ROUND(t.target_total, 2)                    AS target_total,
    ROUND(s.source_total - t.target_total, 2)   AS amount_variance,
    CASE
        WHEN s.source_count = t.target_count
         AND ROUND(s.source_total, 2) = ROUND(t.target_total, 2) THEN 'MATCHED'
        WHEN t.program_code IS NULL                               THEN 'MISSING_IN_TARGET'
        WHEN s.program_code IS NULL                               THEN 'MISSING_IN_SOURCE'
        ELSE 'VARIANCE'
    END AS reconciliation_status
FROM source_summary s
FULL OUTER JOIN target_summary t ON s.program_code = t.program_code
ORDER BY reconciliation_status, ABS(s.source_total - t.target_total) DESC;


-- -------------------------------------------------------
-- STEP 2: Record-level breaks — find exact mismatched records
-- -------------------------------------------------------
SELECT
    s.transaction_id,
    s.partner_id,
    s.program_code,
    s.incentive_amount      AS source_amount,
    t.incentive_amount      AS target_amount,
    s.incentive_amount - t.incentive_amount AS amount_difference,
    s.status                AS source_status,
    t.status                AS target_status,
    CASE
        WHEN t.transaction_id IS NULL THEN 'NOT_IN_TARGET'
        WHEN s.incentive_amount <> t.incentive_amount THEN 'AMOUNT_MISMATCH'
        WHEN s.status <> t.status THEN 'STATUS_MISMATCH'
        ELSE 'OK'
    END AS break_type
FROM edl_transactions s
LEFT JOIN reporting_transactions t ON s.transaction_id = t.transaction_id
WHERE
    s.load_date = CAST(GETDATE() AS DATE)
    AND (
        t.transaction_id IS NULL
        OR s.incentive_amount <> t.incentive_amount
        OR s.status <> t.status
    )
ORDER BY break_type, ABS(s.incentive_amount - COALESCE(t.incentive_amount, 0)) DESC;


-- -------------------------------------------------------
-- STEP 3: Records in target but not in source (phantom records)
-- -------------------------------------------------------
SELECT
    t.transaction_id,
    t.partner_id,
    t.program_code,
    t.incentive_amount,
    t.report_date,
    'PHANTOM_IN_TARGET' AS issue_type
FROM reporting_transactions t
LEFT JOIN edl_transactions s ON t.transaction_id = s.transaction_id
WHERE
    t.report_date = CAST(GETDATE() AS DATE)
    AND s.transaction_id IS NULL;


-- -------------------------------------------------------
-- STEP 4: Reconciliation audit log — track daily recon history
-- (Insert results of Step 1 into an audit table each run)
-- -------------------------------------------------------
INSERT INTO recon_audit_log (
    run_date,
    program_code,
    source_count,
    target_count,
    count_variance,
    source_total,
    target_total,
    amount_variance,
    reconciliation_status
)
SELECT
    CAST(GETDATE() AS DATE),
    program_code,
    source_count,
    target_count,
    count_variance,
    source_total,
    target_total,
    amount_variance,
    reconciliation_status
FROM (
    -- Re-run Step 1 query here or use a view/CTE
    SELECT * FROM v_daily_recon_summary
) recon;
