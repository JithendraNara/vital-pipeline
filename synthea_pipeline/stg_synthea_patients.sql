-- synthea_pipeline/models/omop/stg_synthea_patients.sql
-- Maps Synthea patients.csv → OMOP person table

{{
    config(
        materialized='table',
        schema='omop',
        pre_hook="CREATE SEQUENCE IF NOT EXISTS person_id_seq START 1"
    )
}}

WITH source AS (
    SELECT
        -- Synthea fields
        "Id"                          AS synthea_id,
        "birthdate"                   AS birth_date,
        "deathdate"                   AS death_date,
        "gender"                      AS gender_source,
        "race"                        AS race_source,
        "ethnicity"                   AS ethnicity_source,
        "city"                        AS city,
        "state"                       AS state,
        "zip"                         AS zip_code,
        "income"                      AS income,
        "healthcare_expense"          AS healthcare_expense,
        "healthcare_coverage"         AS healthcare_coverage

    FROM {{ source('staging_synthea', 'patients') }}
),

gender_map AS (
    SELECT 'M' AS source_gender, 8507 AS gender_concept_id UNION ALL
    SELECT 'F', 8532
),

race_map AS (
    SELECT 'white'          AS source_race, 8527 AS race_concept_id UNION ALL
    SELECT 'black',         8516 UNION ALL
    SELECT 'asian',         8515 UNION ALL
    SELECT 'native',        8657 UNION ALL
    SELECT 'other',         8522
),

ethnicity_map AS (
    SELECT 'nonhispanic'    AS source_eth, 38035 AS ethnicity_concept_id UNION ALL
    SELECT 'hispanic',      38036
),

mapped AS (
    SELECT
        s.synthea_id,
        s.birth_date,
        s.death_date,
        -- Calculate age at cohort start (2026-01-01)
        DATE_PART('year', AGE('2026-01-01', s.birth_date::date)) AS age_year,

        g.gender_concept_id,
        r.race_concept_id,
        e.ethnicity_concept_id,

        s.city,
        s.state,
        s.zip_code,
        s.income,
        s.healthcare_expense,
        s.healthcare_coverage,

        -- Year of birth for OMOP
        DATE_PART('year', s.birth_date::date) AS year_of_birth,
        -- Month/day extracted from birthdate
        TO_CHAR(s.birth_date::date, 'MM')    AS month_of_birth,
        TO_CHAR(s.birth_date::date, 'DD')    AS day_of_birth,

        -- Location (state as location — in real OMOP this would be separate location table)
        NULL::integer AS location_id,
        NULL::integer AS provider_id,
        NULL::integer AS care_site_id,

        -- OMOP requires these
        0 AS person_id,  -- populated by sequence
        CURRENT_TIMESTAMP AS last_updated

    FROM source s
    LEFT JOIN gender_map g   ON s.gender_source = g.source_gender
    LEFT JOIN race_map r     ON LOWER(s.race_source) = r.source_race
    LEFT JOIN ethnicity_map e ON LOWER(s.ethnicity_source) = e.source_eth
)

SELECT
    nextval('person_id_seq') AS person_id,
    gender_concept_id,
    year_of_birth,
    month_of_birth,
    day_of_birth,
    race_concept_id,
    ethnicity_concept_id,
    location_id,
    provider_id,
    care_site_id,
    NULL::date AS observation_period_start_date,
    NULL::date AS observation_period_end_date,
    0 AS gender_source_value,
    0 AS race_source_value,
    0 AS ethnicity_source_value,
    NULL::varchar AS race_source_concept_id,
    NULL::varchar AS ethnicity_source_concept_id,
    NULL::integer AS gender_concept_id_ext,
    0 AS condition_occurrence_count,
    0 AS drug_exposure_count,
    0 AS visit_occurrence_count,
    0 AS procedure_occurrence_count,
    0 AS observation_count,
    death_date::date  AS death_date,
    last_updated,
    synthea_id AS person_source_value

FROM mapped
