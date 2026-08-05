{{ config(materialized='ephemeral') }}

select
    opportunity_id,
    opportunity_number,
    opportunity_title,
    opportunity_status,
    close_date,
    updated_at,
    CURRENT_TIMESTAMP() AS opps_gold_processed_at

from {{ ref('silver_grant') }}