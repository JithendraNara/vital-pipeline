-- ============================================================
-- Healthcare Analytics SQL Playbook
-- Window Functions, CTEs, and Healthcare-Specific Queries
-- ============================================================

-- ============================================================
-- QUERY 1: Member Enrollment Span with Recursive CTE
-- Find continuous enrollment periods for each member
-- ============================================================

WITH RECURSIVE enrollment_changes AS (
    -- Base: first enrollment record per member
    SELECT
        mem_id,
        effective_date,
        termination_date,
        plan_type,
        ROW_NUMBER() OVER (PARTITION BY mem_id ORDER BY effective_date) as rn
    FROM members
    WHERE termination_date IS NOT NULL

    UNION ALL

    -- Recursive: carry forward termination date for gap detection
    SELECT
        e.mem_id,
        m.effective_date,
        m.termination_date,
        m.plan_type,
        e.rn + 1
    FROM enrollment_changes e
    JOIN members m
        ON e.mem_id = m.mem_id
        AND m.effective_date > COALESCE(e.termination_date, m.effective_date)
    WHERE e.rn < 10  -- safety limit
),
enrollment_spans AS (
    SELECT
        mem_id,
        MIN(effective_date) as span_start,
        MAX(termination_date) as span_end,
        plan_type,
        COUNT(*) as coverage_segments
    FROM enrollment_changes
    GROUP BY mem_id, plan_type
)
SELECT
    mem_id,
    span_start,
    span_end,
    plan_type,
    coverage_segments,
    DATEDIFF('day', span_start, COALESCE(span_end, CURRENT_DATE)) as days_enrolled,
    CASE
        WHEN span_end IS NULL THEN 'Active'
        WHEN span_end < CURRENT_DATE THEN 'Terminated'
        ELSE 'Active'
    END as status
FROM enrollment_spans
ORDER BY mem_id, span_start;


-- ============================================================
-- QUERY 2: Running Total of Claims Cost by Member
-- Window function: cumulative sum with restart per member
-- ============================================================

SELECT
    mem_id,
    claim_date,
    service_type,
    allowed_amount,
    paid_amount,
    member_copay,
    provider_npi,
    SUM(paid_amount) OVER (
        PARTITION BY mem_id
        ORDER BY claim_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as cumulative_paid,
    SUM(paid_amount) OVER (
        PARTITION BY mem_id
        ORDER BY claim_date
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    ) as rolling_4_claim_paid,
    AVG(paid_amount) OVER (
        PARTITION BY mem_id
        ORDER BY claim_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as avg_paid_to_date,
    ROW_NUMBER() OVER (
        PARTITION BY mem_id ORDER BY claim_date DESC
    ) as claim_seq  -- most recent = 1
FROM medical_claims
WHERE claim_date >= DATE_TRUNC('year', CURRENT_DATE)
ORDER BY mem_id, claim_date;


-- ============================================================
-- QUERY 3: Member Risk Score Calculation (HCC Model Simplified)
-- Window function across ICD-10 diagnosis history
-- ============================================================

WITH diagnosis_history AS (
    SELECT
        m.mem_id,
        m.dob,
        c.claim_id,
        c.claim_date,
        d.icd10_code,
        d.description,
        d.hcc_code,
        d.hcc_weight,
        -- Rank diagnoses per member (most recent first)
        ROW_NUMBER() OVER (
            PARTITION BY m.mem_id, d.icd10_code
            ORDER BY c.claim_date DESC
        ) as diagnosis_rank,
        -- Count unique HCCs per member in last 12 months
        COUNT(DISTINCT d.hcc_code) OVER (
            PARTITION BY m.mem_id
            WHERE c.claim_date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '12 months'
        ) as active_hcc_count
    FROM members m
    JOIN medical_claims c ON m.mem_id = c.mem_id
    JOIN diagnoses d ON c.claim_id = d.claim_id
    WHERE c.claim_date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '12 months'
),
member_risk AS (
    SELECT
        mem_id,
        dob,
        SUM(hcc_weight) as total_hcc_weight,
        active_hcc_count,
        -- Risk adjustment factor based on demographics (simplified)
        CASE
            WHEN MONTHS_BETWEEN(CURRENT_DATE, dob) / 12 < 45 THEN 1.0
            WHEN MONTHS_BETWEEN(CURRENT_DATE, dob) / 12 < 65 THEN 1.4
            ELSE 2.0
        END as demographic_factor,
        ROUND(SUM(hcc_weight) * (
            CASE
                WHEN MONTHS_BETWEEN(CURRENT_DATE, dob) / 12 < 45 THEN 1.0
                WHEN MONTHS_BETWEEN(CURRENT_DATE, dob) / 12 < 65 THEN 1.4
                ELSE 2.0
            END
        ), 2) as risk_score
    FROM diagnosis_history
    WHERE diagnosis_rank = 1  -- Most recent diagnosis per code
    GROUP BY mem_id, dob
)
SELECT
    mem_id,
    dob,
    total_hcc_weight,
    active_hcc_count,
    demographic_factor,
    risk_score,
    CASE
        WHEN risk_score < 1.0 THEN 'Low'
        WHEN risk_score < 2.0 THEN 'Moderate'
        WHEN risk_score < 3.0 THEN 'High'
        ELSE 'Very High'
    END as risk_category
FROM member_risk
ORDER BY risk_score DESC
LIMIT 100;


-- ============================================================
-- QUERY 4: PMPM (Per Member Per Month) Cost Analysis
-- Standard VBC metric with month-over-month trend
-- ============================================================

WITH member_months AS (
    SELECT DISTINCT
        m.mem_id,
        DATE_TRUNC('month', c.claim_date) as claim_month,
        m.plan_type,
        m.metal_level,
        m.state
    FROM members m
    JOIN medical_claims c ON m.mem_id = c.mem_id
    WHERE c.claim_date >= DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '12 months'
),
monthly_costs AS (
    SELECT
        mm.claim_month,
        mm.plan_type,
        mm.metal_level,
        COUNT(DISTINCT mm.mem_id) as member_count,
        SUM(c.paid_amount) as total_paid,
        SUM(c.allowed_amount) as total_allowed,
        SUM(c.member_copay) as total_copay,
        SUM(c.allowed_amount) / NULLIF(COUNT(DISTINCT mm.mem_id), 0) as allowed_per_member,
        SUM(c.paid_amount) / NULLIF(COUNT(DISTINCT mm.mem_id), 0) as paid_per_member
    FROM member_months mm
    JOIN medical_claims c ON mm.mem_id = c.mem_id
    GROUP BY mm.claim_month, mm.plan_type, mm.metal_level
)
SELECT
    claim_month,
    plan_type,
    metal_level,
    member_count,
    total_paid,
    total_allowed,
    ROUND(paid_per_member, 2) as pmpm_paid,
    ROUND(allowed_per_member, 2) as pmpm_allowed,
    LAG(pmpm_paid) OVER (
        PARTITION BY plan_type, metal_level ORDER BY claim_month
    ) as prev_month_pmpm,
    ROUND(
        (pmpm_paid - LAG(pmpm_paid) OVER (
            PARTITION BY plan_type, metal_level ORDER BY claim_month
        )) / NULLIF(LAG(pmpm_paid) OVER (
            PARTITION BY plan_type, metal_level ORDER BY claim_month
        ), 0) * 100, 1
    ) as mom_change_pct
FROM monthly_costs
ORDER BY claim_month DESC, plan_type, metal_level;


-- ============================================================
-- QUERY 5: Provider Attribution with Tier Classification
-- Which providers drive highest cost, and at what tier
-- ============================================================

WITH provider_metrics AS (
    SELECT
        p.provider_npi,
        p.provider_name,
        p.specialty,
        p.tier,  -- 1=in-network preferred, 2=in-network, 3=out-of-network
        COUNT(DISTINCT c.claim_id) as total_claims,
        COUNT(DISTINCT c.mem_id) as unique_members,
        SUM(c.paid_amount) as total_paid,
        SUM(c.allowed_amount) as total_allowed,
        SUM(c.member_copay) as total_copay,
        AVG(c.paid_amount) as avg_paid_per_claim,
        AVG(c.allowed_amount) as avg_allowed_per_claim,
        SUM(c.allowed_amount) / NULLIF(COUNT(DISTINCT c.mem_id), 0) as cost_per_member,
        -- Rank within specialty by total paid
        RANK() OVER (
            PARTITION BY p.specialty ORDER BY SUM(c.paid_amount) DESC
        ) as cost_rank_in_specialty,
        -- Rank within tier
        RANK() OVER (
            PARTITION BY p.tier ORDER BY SUM(c.paid_amount) DESC
        ) as cost_rank_in_tier
    FROM providers p
    JOIN medical_claims c ON p.provider_npi = c.provider_npi
    WHERE c.claim_date >= DATE_TRUNC('year', CURRENT_DATE)
    GROUP BY p.provider_npi, p.provider_name, p.specialty, p.tier
)
SELECT
    provider_npi,
    provider_name,
    specialty,
    tier,
    total_claims,
    unique_members,
    ROUND(total_paid, 2) as total_paid,
    ROUND(cost_per_member, 2) as cost_per_member,
    avg_paid_per_claim,
    cost_rank_in_specialty,
    cost_rank_in_tier,
    -- Flag high-cost providers for review
    CASE
        WHEN cost_rank_in_specialty <= 3 THEN '🔴 High-Cost Specialty'
        WHEN tier = 3 THEN '🟡 Out-of-Network'
        WHEN cost_rank_in_tier <= 5 THEN '🟠 High-Cost Tier'
        ELSE '✅ Normal'
    END as flag
FROM provider_metrics
WHERE cost_rank_in_specialty <= 10 OR cost_rank_in_tier <= 10
ORDER BY total_paid DESC;


-- ============================================================
-- QUERY 6: Readmission Analysis (CMS HAI Measure)
-- 30-day all-cause hospital readmission rate by facility
-- ============================================================

WITH inpatient_claims AS (
    SELECT
        c.claim_id,
        c.mem_id,
        c.provider_npi,
        p.facility_name,
        p.facility_state,
        c.admit_date,
        c.discharge_date,
        c.primary_diagnosis,
        c.drg_code,
        c.paid_amount,
        LEAD(c.admit_date) OVER (
            PARTITION BY c.mem_id ORDER BY c.admit_date
        ) as next_admit_date,
        LEAD(c.primary_diagnosis) OVER (
            PARTITION BY c.mem_id ORDER BY c.admit_date
        ) as next_primary_diagnosis
    FROM medical_claims c
    JOIN providers p ON c.provider_npi = p.provider_npi
    WHERE c.claim_type = 'inpatient'
        AND c.admit_date >= DATE_TRUNC('year', CURRENT_DATE)
),
readmissions AS (
    SELECT
        claim_id,
        mem_id,
        facility_name,
        facility_state,
        admit_date,
        discharge_date,
        drg_code,
        paid_amount,
        -- Readmission if next admission within 30 days
        CASE
            WHEN next_admit_date IS NOT NULL
             AND DATEDIFF('day', discharge_date, next_admit_date) <= 30
            THEN 1
            ELSE 0
        END as is_30day_readmission,
        DATEDIFF('day', discharge_date, next_admit_date) as days_to_readmission
    FROM inpatient_claims
)
SELECT
    facility_name,
    facility_state,
    COUNT(*) as total_discharges,
    SUM(is_30day_readmission) as readmissions,
    ROUND(
        SUM(is_30day_readmission) * 100.0 / COUNT(*),
        2
    ) as readmission_rate_pct,
    ROUND(AVG(paid_amount), 2) as avg_admission_cost,
    ROUND(AVG(CASE WHEN is_30day_readmission = 1 THEN paid_amount END), 2) as avg_readmission_cost
FROM readmissions
GROUP BY facility_name, facility_state
HAVING COUNT(*) >= 10  -- Suppress rates with <10 cases
ORDER BY readmission_rate_pct DESC
LIMIT 25;
