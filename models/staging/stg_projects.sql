with
source as (
    select * from {{ source('somsolet', 'somsolet_project') }}
),

renamed as (
    select
        id as project_id,
        name as project_name,
        status,
        campaign_id,
        discarded_type,
        preregistration_date,
        registration_date,
        date_report,
        date_prereport,
        date_start_installation,
        date_legal_docs
    from source
)

select * from renamed
