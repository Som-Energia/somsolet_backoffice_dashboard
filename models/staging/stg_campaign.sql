with
source as (
    select * from {{ source('somsolet', 'somsolet_campaign') }}
),

renamed as (
    select
        id as campaign_id,
        name as campaign_name,
        active
    from source
)

select * from renamed
