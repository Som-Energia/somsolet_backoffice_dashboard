{{ config(materialized='view') }}

select
    projects.*,
    campaigns.campaign_name,
    case
        when
            projects.status = 'discarded'
            and projects.discarded_type = 'technical' then 'discarded_technical'
        when
            projects.status = 'discarded'
            and projects.discarded_type = 'voluntary' then 'discarded_voluntary'
    end as discarded_hiper_type,
    case
        when
            projects.status = 'discarded'
            and projects.discarded_type = 'voluntary'
            and projects.date_report is null
            then 'discarded_voluntary_before_technical_visit'
        when
            projects.status = 'discarded'
            and projects.discarded_type = 'voluntary'
            and projects.date_report is not null
            then 'discarded_voluntary_after_technical_visit'
    end as discarded_voluntary_moment
from {{ ref('stg_projects') }} as projects
inner join
    {{ ref('stg_campaign') }} as campaigns
    on projects.campaign_id = campaigns.campaign_id
