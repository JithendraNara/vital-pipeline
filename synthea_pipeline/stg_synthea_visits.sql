-- synthea_pipeline/models/omop/stg_synthea_visits.sql
-- Maps Synthea encounters.csv → OMOP visit_occurrence

{{ config(materialized='table', schema='omop') }}

WITH source AS (
    SELECT
        "Id"              AS encounter_id,
        "PATIENT"         AS patient_id,
        "ENCOUNTERCLASS"  AS visit_class,
        "START"           AS visit_start_date,
        "STOP"            AS visit_end_date,
        "CODE"            AS cpt_code,
        "DESCRIPTION"     AS visit_description,
        "REASONDESCRIPTION" AS reason_description

    FROM {{ source('staging_synthea', 'encounters') }}
),

-- Synthea encounterclass → OMOP visit_concept_id
-- OMOP vocabulary: visit_concept_id values
visit_class_map AS (
    SELECT 'encounter'       AS source_class, 44814707 AS visit_concept_id UNION ALL
    SELECT 'emergency',                        9203 UNION ALL
    SELECT 'inpatient',                        9201 UNION ALL
    SELECT 'outpatient',                       9202 UNION ALL
    SELECT 'ambulatory',                       44814707 UNION ALL
    SELECT 'wellcare',                         44814708 UNION ALL
    SELECT 'telehealth',                       508142 UNION ALL
    SELECT 'urgentcare',                       9203  -- maps to emergency
),

mapped AS (
    SELECT
        e.encounter_id,
        e.patient_id,
        v.visit_concept_id,
        e.visit_start_date::timestamp    AS visit_start_datetime,
        e.visit_end_date::timestamp      AS visit_end_datetime,

        -- Calculate length of stay (in days)
        CASE
            WHEN e.visit_end_date IS NOT NULL
            THEN DATE_PART('day', e.visit_end_date::timestamp - e.visit_start_date::timestamp)
            ELSE 0
        END AS visit_los,

        e.cpt_code,
        e.visit_description,
        e.reason_description,
        e.visit_class AS visit_source_value,
        NULL::integer  AS provider_id,
        NULL::integer  AS care_site_id,

        -- OMOP standard type concepts
        CASE v.visit_concept_id
            WHEN 9201 THEN 32035  -- Visit Occurrence — inpatient
            WHEN 9203 THEN 32036  -- Visit Occurrence — emergency
            ELSE 44814717         -- Visit Occurrence — outpatient
        END AS visit_type_concept_id,

        CURRENT_TIMESTAMP AS last_updated

    FROM source e
    LEFT JOIN visit_class_map v ON e.visit_class = v.source_class
)

SELECT * FROM mapped
