-- Intermediate: member months
-- Calculate coverage months per member for PMPM calculations
{{ config(
    materialized='view',
    schema='intermediate'
) }}

WITH members_with_months AS (
    SELECT
        m.member_id,
        m.first_name,
        m.last_name,
        m.date_of_birth,
        m.plan_type,
        m.metal_level,
        m.state,
        -- Generate all months from effective to today (or termination)
        GENERATE_SERIES(
            DATE_TRUNC('month', m.coverage_effective_date),
            CASE
                WHEN m.coverage_termination_date IS NULL
                    THEN DATE_TRUNC('month', CURRENT_DATE)
                ELSE DATE_TRUNC('month', m.coverage_termination_date)
            END,
            INTERVAL '1 month'
        )::DATE AS coverage_month,
        -- Calculate age
        DATE_PART('year', AGE(m.date_of_birth)) AS age,
        -- Dependents
        CASE WHEN m.relationship = 'Self' THEN TRUE ELSE FALSE END AS is_primary
    FROM {{ ref('stg_eligibility_members') }} m
    WHERE
        -- Only active members
        m.coverage_termination_date >= CURRENT_DATE
        OR m.coverage_termination_date IS NULL
)
SELECT
    member_id,
    first_name,
    last_name,
    date_of_birth,
    age,
    plan_type,
    metal_level,
    state,
    coverage_month,
    is_primary,
    -- Member month = 1 for each month a member is active
    1 AS member_months
FROM members_with_months
