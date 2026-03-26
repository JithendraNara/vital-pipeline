-- synthea_pipeline/models/omop/stg_synthea_conditions.sql
-- Maps Synthea conditions.csv → OMOP condition_occurrence
-- ICD-10-CM codes → SNOMED-CT via concept mapping

{{ config(materialized='table', schema='omop') }}

WITH source AS (
    SELECT
        "Id"              AS encounter_id,
        "START"           AS condition_start_date,
        "STOP"            AS condition_end_date,
        "PATIENT"         AS patient_id,
        "CODE"            AS icd10_code,
        "DESCRIPTION"     AS condition_description

    FROM {{ source('staging_synthea', 'conditions') }}
),

-- ICD-10-CM → SNOMED-CT mapping
-- In production: join to omop_vocab.concept with vocabulary_id = 'ICD10CM' / 'SNOMED'
icd10_to_snomed AS (
    SELECT
        icd10_code,
        CASE icd10_code
            WHEN 'E11.9'  THEN 319844   -- Type 2 diabetes mellitus without complications → SNOMED
            WHEN 'I10'    THEN 4027541   -- Essential hypertension → SNOMED
            WHEN 'J44.1'  THEN 255848    -- COPD → SNOMED
            WHEN 'M54.5'  THEN 3946111   -- Low back pain → SNOMED
            WHEN 'F32.9'  THEN 3520530   -- Depression → SNOMED
            WHEN 'R51'    THEN 4039357   -- Headache → SNOMED
            WHEN 'K21.0'  THEN 4000961   -- GERD → SNOMED
            WHEN 'E78.5'  THEN 4013720   -- Hyperlipidemia → SNOMED
            WHEN 'I25.10' THEN 4052490   -- Heart disease → SNOMED
            WHEN 'Z23'    THEN 4302132   -- Immunization → SNOMED
            WHEN 'J45.909' THEN 259153  -- Asthma → SNOMED
            WHEN 'N18.3'  THEN 192623   -- CKD → SNOMED
            WHEN 'R73.03' THEN 3004418  -- Prediabetes → SNOMED
            ELSE NULL
        END AS condition_concept_id,

        -- Also capture ICD-10-CM concept for source tracking
        CASE icd10_code
            WHEN 'E11.9'  THEN 44828250
            WHEN 'I10'    THEN 320128
            WHEN 'J44.1'  THEN 257867
            WHEN 'M54.5'  THEN 4029483
            WHEN 'F32.9'  THEN 3520530
            WHEN 'R51'    THEN 4039357
            WHEN 'K21.0'  THEN 4000961
            WHEN 'E78.5'  THEN 4013720
            WHEN 'I25.10' THEN 4052490
            WHEN 'Z23'    THEN 4302132
            WHEN 'J45.909' THEN 256675
            WHEN 'N18.3'  THEN 192623
            WHEN 'R73.03' THEN 3004418
            ELSE NULL
        END AS condition_source_concept_id

    FROM source
),

mapped AS (
    SELECT
        s.encounter_id,
        s.condition_start_date::date  AS condition_start_date,
        s.condition_end_date::date    AS condition_end_date,
        s.patient_id                  AS person_id,
        i.condition_concept_id,
        i.condition_source_concept_id,
        s.icd10_code                  AS condition_source_value,
        'ICD10CM'                     AS condition_type_concept_id,
        0                             AS condition_status_concept_id,
        NULL::integer                 AS stop_reason,
        CURRENT_TIMESTAMP             AS last_updated

    FROM source s
    LEFT JOIN icd10_to_snomed i ON s."CODE" = i.icd10_code
)

SELECT * FROM mapped
