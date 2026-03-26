-- synthea_pipeline/models/marts/mart_condition_prevalence.sql
-- OMOP condition prevalence by age group, gender, and geography
-- Primary output for observational research on condition distribution

{{ config(materialized='table', schema='marts') }}

WITH conditions AS (
    SELECT * FROM {{ ref('stg_synthea_conditions') }}
),

patients AS (
    SELECT * FROM {{ ref('stg_synthea_patients') }}
),

enriched AS (
    SELECT
        c.condition_source_value     AS icd10_code,
        c.condition_concept_id,
        c.condition_start_date,

        p.person_id,
        p.age_year,
        p.gender_concept_id,
        p.state,
        p.death_date

        -- Age buckets
        , CASE
            WHEN p.age_year < 18  THEN '0-17'
            WHEN p.age_year < 30  THEN '18-29'
            WHEN p.age_year < 45  THEN '30-44'
            WHEN p.age_year < 60  THEN '45-59'
            WHEN p.age_year < 75  THEN '60-74'
            ELSE '75+'
        END AS age_bucket

        -- Gender label
        , CASE p.gender_concept_id
            WHEN 8507  THEN 'Male'
            WHEN 8532  THEN 'Female'
            ELSE 'Unknown'
        END AS gender

        -- Active condition (not ended/deceased)
        , CASE
            WHEN c.condition_end_date IS NULL
             AND (p.death_date IS NULL OR c.condition_start_date <= p.death_date)
            THEN 1
            ELSE 0
        END AS is_active

    FROM conditions c
    JOIN patients p ON c.person_id = p.synthea_id
),

aggregated AS (
    SELECT
        icd10_code                           AS condition_code,
        age_bucket,
        gender,
        state,
        COUNT(DISTINCT person_id)            AS patient_count,
        COUNT(*)                            AS total_occurrences,
        SUM(is_active)                       AS active_count,
        AVG(EXTRACT(DOY FROM condition_start_date)::numeric) AS avg_day_of_year,
        COUNT(DISTINCT person_id) * 1.0
            / NULLIF(SUM(COUNT(DISTINCT person_id)) OVER (), 0) * 100  AS pct_of_cohort

    FROM enriched
    GROUP BY icd10_code, age_bucket, gender, state
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY age_bucket, gender ORDER BY patient_count DESC) AS rank_in_segment
    FROM aggregated
)

SELECT
    condition_code,
    age_bucket,
    gender,
    state,
    patient_count,
    total_occurrences,
    active_count,
    ROUND(pct_of_cohort, 4) AS pct_of_cohort,
    rank_in_segment

FROM ranked
ORDER BY age_bucket, gender, rank_in_segment
