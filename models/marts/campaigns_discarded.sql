{{ config(materialized='view') }}

select
    projects.*,
    campaigns.campaign_name,
    case
        when
            projects.status = 'discarded'
            and projects.discarded_type = 'technical' then 'Tècnics'
        when
            projects.status = 'discarded'
            and projects.discarded_type = 'voluntary' then 'Voluntaris'
    end as discarded_hiper_type,
    case
        when
            projects.status = 'discarded'
            and projects.discarded_type = 'voluntary'
            and projects.date_report is null
            then 'Voluntari abans visita tècnica'
        when
            projects.status = 'discarded'
            and projects.discarded_type = 'voluntary'
            and projects.date_report is not null
            then 'Voluntari després de visita tècnica'
    end as discarded_voluntary_moment
from {{ ref('stg_projects') }} as projects
inner join
    {{ ref('stg_campaign') }} as campaigns
    on projects.campaign_id = campaigns.campaign_id
