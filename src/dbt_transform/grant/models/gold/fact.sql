{{ config(materialized='table') }}

select
    opportunity_id,      
    agency_code,        
    post_date,
    close_date,
    award_ceiling,
    award_floor,
from {{ ref('silver_grant') }}
