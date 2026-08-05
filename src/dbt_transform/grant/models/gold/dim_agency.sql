{{ config(materialized='table') }}

select distinct
    agency_code,
    agency_name,
    top_level_agency_code,
    top_level_agency_name,
    current_timestamp() as opps_gold_processed_at
from {{ ref('silver_grant') }}
where agency_code is not null