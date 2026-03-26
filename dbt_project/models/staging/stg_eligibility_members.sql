-- Staging: eligibility members
-- Raw → cleaned, typed, consistent column names
{{ config(
    materialized='view',
    schema='staging'
) }}

WITH source AS (
    SELECT
        mem_id,
        first_name,
        last_name,
        dob,
        email,
        phone,
        address,
        city,
        state,
        zip_code,
        effective_date,
        termination_date,
        covered_relation,
        plan_type,
        metal_level,
        hsa_eligible
    FROM {{ source('raw', 'eligibility') }}
),
cleaned AS (
    SELECT
        -- Natural key
        mem_id::VARCHAR(50) AS member_id,
        -- Demographics
        first_name::VARCHAR(100) AS first_name,
        last_name::VARCHAR(100) AS last_name,
        dob::DATE AS date_of_birth,
        email::VARCHAR(255) AS email,
        phone::VARCHAR(20) AS phone,
        -- Address
        address::VARCHAR(255) AS address,
        city::VARCHAR(100) AS city,
        state::VARCHAR(2) AS state,
        -- Zip: zero-pad to 5 digits
        LPAD(TRIM(zip_code::VARCHAR), 5, '0') AS zip_code,
        -- Dates
        effective_date::DATE AS coverage_effective_date,
        -- Termination: null if 'Active'
        NULLIF(termination_date, 'Active')::DATE AS coverage_termination_date,
        -- Dependents
        covered_relation::VARCHAR(50) AS relationship,
        -- Plan
        UPPER(plan_type)::VARCHAR(10) AS plan_type,
        metal_level::VARCHAR(20) AS metal_level,
        -- Flags
        CASE WHEN hsa_eligible = 'Yes' THEN TRUE ELSE FALSE END AS hsa_eligible
    FROM source
)
SELECT * FROM cleaned
