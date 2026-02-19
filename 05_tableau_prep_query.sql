-- ============================================================
-- TABLEAU DASHBOARD PREP QUERY
-- Use Case: Optimized data extract for Tableau dashboards
--           tracking incentive performance and partner trends.
--           Designed for performance on large datasets.
-- Author: Anna Rupa Anthony
-- ============================================================


WITH

-- Base transaction data with date dimensions
base AS (
    SELECT
        t.transaction_id,
        t.partner_id,
        t.program_code,
        t.incentive_amount,
        t.status,
        t.transaction_date,
        t.settlement_date,
        YEAR(t.transaction_date)                            AS txn_year,
        MONTH(t.transaction_date)                           AS txn_month,
        DATENAME(MONTH, t.transaction_date)                 AS txn_month_name,
        DATEPART(QUARTER, t.transaction_date)               AS txn_quarter,
        CAST(DATEADD(DAY,
            1 - DATEPART(WEEKDAY, t.transaction_date),
            t.transaction_date) AS DATE)                    AS week_start,
        DATEDIFF(DAY, t.transaction_date, t.settlement_date) AS days_to_settle
    FROM transactions t
    WHERE t.transaction_date >= DATEADD(YEAR, -2, GETDATE())  -- 2-year rolling window
),

-- Partner details
partner_info AS (
    SELECT
        p.partner_id,
        p.partner_name,
        p.partner_tier,         -- e.g. Gold, Silver, Bronze
        p.region,
        p.country,
        p.partner_type          -- e.g. Reseller, Distributor, Retailer
    FROM partners p
    WHERE p.active_flag = 1
),

-- Program details
program_info AS (
    SELECT
        pr.program_code,
        pr.program_name,
        pr.program_type,        -- e.g. Volume, Growth, MDF
        pr.start_date,
        pr.end_date,
        pr.budget_amount
    FROM programs pr
),

-- Month-over-month metrics for trend analysis
monthly_metrics AS (
    SELECT
        program_code,
        txn_year,
        txn_month,
        txn_month_name,
        COUNT(*)                    AS transaction_count,
        SUM(incentive_amount)       AS total_incentive,
        AVG(incentive_amount)       AS avg_incentive,
        COUNT(DISTINCT partner_id)  AS active_partners,
        SUM(CASE WHEN status = 'SETTLED'   THEN incentive_amount ELSE 0 END) AS settled_amount,
        SUM(CASE WHEN status = 'PENDING'   THEN incentive_amount ELSE 0 END) AS pending_amount,
        SUM(CASE WHEN status = 'ERROR'     THEN 1 ELSE 0 END)               AS error_count
    FROM base
    GROUP BY program_code, txn_year, txn_month, txn_month_name
)

-- FINAL OUTPUT: Denormalized, Tableau-ready flat table
SELECT
    b.transaction_id,
    b.transaction_date,
    b.txn_year,
    b.txn_month,
    b.txn_month_name,
    b.txn_quarter,
    b.week_start,

    -- Partner dimensions
    b.partner_id,
    pi.partner_name,
    pi.partner_tier,
    pi.region,
    pi.country,
    pi.partner_type,

    -- Program dimensions
    b.program_code,
    pr.program_name,
    pr.program_type,
    pr.budget_amount                                        AS program_budget,

    -- Transaction metrics
    b.incentive_amount,
    b.status,
    b.days_to_settle,
    CASE
        WHEN b.days_to_settle <= 3  THEN 'On-Time'
        WHEN b.days_to_settle <= 7  THEN 'Slight Delay'
        WHEN b.days_to_settle <= 14 THEN 'Delayed'
        ELSE                             'Critical Delay'
    END                                                     AS settlement_tier,

    -- Month-level aggregates (for Tableau LOD calculations)
    mm.transaction_count                                    AS monthly_txn_count,
    mm.total_incentive                                      AS monthly_total_incentive,
    mm.active_partners                                      AS monthly_active_partners,
    mm.error_count                                          AS monthly_error_count,

    -- Budget utilization
    ROUND(
        100.0 * mm.total_incentive / NULLIF(pr.budget_amount, 0), 2
    )                                                       AS budget_utilization_pct

FROM base b
LEFT JOIN partner_info  pi ON b.partner_id    = pi.partner_id
LEFT JOIN program_info  pr ON b.program_code  = pr.program_code
LEFT JOIN monthly_metrics mm
    ON b.program_code = mm.program_code
    AND b.txn_year    = mm.txn_year
    AND b.txn_month   = mm.txn_month
ORDER BY b.transaction_date DESC;
