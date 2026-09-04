{{ config(materialized='table') }}

select
    s.opportunity_id,
    s.opportunity_number,
    s.opportunity_title,
    s.opportunity_url,
    s.summary_description,
    s.applicant_eligibility,
    e.research_domain,                    -- refined LLM category, joined in
    s.opportunity_status,
    s.close_date,
    s.updated_at,
    CURRENT_TIMESTAMP() AS opps_gold_processed_at

from {{ ref('silver_grant') }} s
left join {{ source('silver', 'enrichment_grant_category') }} e
    on s.opportunity_id = e.opportunity_id