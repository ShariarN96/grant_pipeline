{{ config(
    materialized='incremental',
    unique_key='opportunity_id'
) }}

WITH source AS (
    SELECT
        -- identifiers
        raw_data:opportunity_id::STRING                       AS opportunity_id,
        raw_data:opportunity_number::STRING                   AS opportunity_number,
        raw_data:opportunity_status::STRING                   AS opportunity_status,
        raw_data:opportunity_title::STRING                    AS opportunity_title,
        CASE
            WHEN raw_data:opportunity_id::STRING IS NOT NULL
                THEN 'https://simpler.grants.gov/opportunity/' || raw_data:opportunity_id::STRING
            WHEN raw_data:legacy_opportunity_id::STRING IS NOT NULL
                THEN 'https://grants.gov/search-results-detail/' || raw_data:legacy_opportunity_id::STRING
            ELSE NULL
        END AS opportunity_url,

        -- agency (becomes dim_agency in gold)
        raw_data:agency::STRING                               AS agency_code,
        raw_data:agency_name::STRING                          AS agency_name,
        raw_data:top_level_agency_code::STRING                AS top_level_agency_code,
        raw_data:top_level_agency_name::STRING                AS top_level_agency_name,

        -- money / counts (measures)
        raw_data:summary:award_ceiling::NUMBER                AS award_ceiling,
        raw_data:summary:award_floor::NUMBER                  AS award_floor,
        raw_data:summary:estimated_total_program_funding::NUMBER AS estimated_total_program_funding,
        raw_data:summary:expected_number_of_awards::NUMBER    AS expected_number_of_awards,

        -- dates
        raw_data:summary:post_date::DATE                      AS post_date,
        raw_data:summary:close_date::DATE                     AS close_date,
        raw_data:summary:updated_at::TIMESTAMP                AS updated_at,

        -- descriptive text
        raw_data:summary:additional_info_url::STRING          AS additional_info_url,
        raw_data:summary:agency_contact_description::STRING   AS agency_contact,
        raw_data:summary:agency_email_address::STRING         AS agency_email,
        raw_data:summary:applicant_eligibility_description::STRING AS applicant_eligibility,
        raw_data:summary:close_date_description::STRING       AS close_date_description,
        -- raw_data:summary:funding_categories::STRING AS funding_category,
        ARRAY_TO_STRING(raw_data:summary:funding_categories, ', ') AS funding_category,
        raw_data:summary:funding_category_description::STRING AS funding_category_description,
        raw_data:summary:summary_description::STRING          AS summary_description,

        -- lineage
        source_file,
        ingestion_time,
        current_timestamp() as silver_processed_at

    FROM bronze.bronze_grants

    {% if is_incremental() %}
        WHERE ingestion_time >= (SELECT COALESCE(MAX(ingestion_time), '1900-01-01') FROM {{ this }})
    {% endif %}
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY opportunity_id
            ORDER BY updated_at DESC NULLS LAST, ingestion_time DESC
        ) AS rn
    FROM source
)

SELECT * EXCLUDE (rn)
FROM deduped
WHERE rn = 1