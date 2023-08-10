{% set campaign_id = 66 %}

select
    campaign_id,
    count(*) filter (
        where status in (
            'end installation',
            'legal registration',
            'legalization',
            'warranty payment'
        )
    ) as total_executed,
    count(*) filter (where status = 'discarded') as discarded,
    count(*) as inscriptions,
    count(*) filter (
        where status = 'discarded' and discarded_type = 'technical'
    ) as discarded_technical,
    count(*) filter (
        where status = 'discarded' and discarded_type = 'voluntary'
    ) as discarded_voluntary,
    count(*) filter (
        where status = 'discarded'
        and discarded_type = 'voluntary'
        and date_report is null
    ) as discarded_voluntary_before_technical_visit,
    count(*) filter (
        where status = 'discarded'
        and discarded_type = 'voluntary'
        and date_report is not null
    ) as discarded_voluntary_after_technical_visit,
    count(*) filter (where status = 'registered') as pending_prereport,
    count(*) filter (where status = 'prereport') as pending_technical_visit,
    count(*) filter (
        where status = 'technical visit'
    ) as pending_technical_scheduled_visit,
    count(*) filter (where status in (
        'report', 'report review',
        'offer', 'offer review',
        'signature',
        'construction',
        'date installation set',
        'end installation',
        'legal registration',
        'legalization',
        'warranty payment'
    ))
    + count(*) filter (
        where status = 'discarded' and discarded_type = 'technical'
    ) as total_technical_visits_done,
    count(*) filter (
        where status in ('report', 'report review')
    ) as technical_visit_report,
    count(*) filter (where status in ('offer', 'offer review')) as offers_sent,
    count(*) filter (where status in (
        'signature',
        'construction',
        'date installation set',
        'end installation',
        'legal registration',
        'legalization',
        'warranty payment'
    )) as total_signed_contracts,
    count(*) filter (where status = 'signature') as signed_contracts,
    count(*) filter (
        where status = 'construction permit'
    ) as pending_license_response,
    count(*) filter (where status = 'date installation set') as scheduled_works,
    count(*) filter (where status = 'end installation') as works_completed,
    count(*) filter (where status = 'legal registration') as legalization_stage,
    count(*) filter (where status = 'legalization') as legalization_completed,
    count(*) filter (where status in (
        'end installation',
        'legal registration',
        'legalization',
        'warranty payment'
    )) as total_executed_works,
    count(*) filter (
        where status = 'warranty payment'
    ) as warranty_payment_done,
    avg(
        case
            when
                preregistration_date is not null
                then abs((preregistration_date - date_start_installation))
            when
                registration_date is not null
                then abs((registration_date - date_start_installation))
            else abs((date_prereport - date_start_installation))
        end
    ) as average_days_to_start_installation,
    avg(
        case
            when
                preregistration_date is not null
                then abs((preregistration_date - date_legal_docs))
            when
                registration_date is not null
                then abs((registration_date - date_legal_docs))
            else abs((date_prereport - date_legal_docs))
        end
    ) as average_days_to_legalize_installation,
    ((
        (
            count(*) filter (
                where status in (
                    'end installation',
                    'legal registration',
                    'legalization',
                    'warranty payment'
                )
            )::decimal
            + count(*) filter (where status = 'discarded')::decimal
        )
        / count(*)::decimal
    ) * 100) as percentage_executed_and_discarded
from {{ source('somsolet', 'somsolet_project') }}
-- where campaign_id = {{ campaign_id }}
group by campaign_id
