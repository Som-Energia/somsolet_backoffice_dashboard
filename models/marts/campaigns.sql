with projects as (
    select * from {{ ref('stg_projects') }}
),

campaigns as (
    select * from {{ ref('stg_campaign') }}
),

campaign_summary as (
    select
        campaigns.campaign_name as campaign_name,
        count(*) filter (
            where projects.status in (
                'end installation',
                'legal registration',
                'legalization',
                'warranty payment'
            )
        ) as total_executed,

        count(*) filter (where projects.status = 'discarded') as discarded,
        count(*) as inscriptions,
        count(*) filter (
            where projects.status = 'discarded'
            and projects.discarded_type = 'technical'
        ) as discarded_technical,
        count(*) filter (
            where projects.status = 'discarded'
            and projects.discarded_type = 'voluntary'
        ) as discarded_voluntary,
        count(*) filter (
            where projects.status = 'discarded'
            and projects.discarded_type = 'voluntary'
            and projects.date_report is null
        ) as discarded_voluntary_before_technical_visit,
        count(*) filter (
            where projects.status = 'discarded'
            and projects.discarded_type = 'voluntary'
            and projects.date_report is not null
        ) as discarded_voluntary_after_technical_visit,
        count(*) filter (
            where projects.status = 'registered'
        ) as pending_prereport,
        count(*) filter (
            where projects.status = 'prereport'
        ) as pending_technical_visit,
        count(*) filter (
            where projects.status = 'technical visit'
        ) as pending_technical_scheduled_visit,
        count(*) filter (where projects.status in (
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
            where projects.status = 'discarded'
            and projects.discarded_type = 'technical'
        ) as total_technical_visits_done,
        count(*) filter (
            where projects.status in ('report', 'report review')
        ) as technical_visit_report,
        count(*) filter (
            where projects.status in ('offer', 'offer review')
        ) as offers_sent,
        count(*) filter (where projects.status in (
            'signature',
            'construction',
            'date installation set',
            'end installation',
            'legal registration',
            'legalization',
            'warranty payment'
        )) as total_signed_contracts,
        count(*) filter (
            where projects.status = 'signature'
        ) as signed_contracts,
        count(*) filter (
            where projects.status = 'construction permit'
        ) as pending_license_response,
        count(*) filter (
            where projects.status = 'date installation set'
        ) as scheduled_works,
        count(*) filter (
            where projects.status = 'end installation'
        ) as works_completed,
        count(*) filter (
            where projects.status = 'legal registration'
        ) as legalization_stage,
        count(*) filter (
            where projects.status = 'legalization'
        ) as legalization_completed,
        count(*) filter (where projects.status in (
            'end installation',
            'legal registration',
            'legalization',
            'warranty payment'
        )) as total_executed_works,
        count(*) filter (
            where projects.status = 'warranty payment'
        ) as warranty_payment_done,
        avg(
            case
                when
                    projects.preregistration_date is not null
                    then
                        abs(
                            (
                                projects.preregistration_date
                                - projects.date_start_installation
                            )
                        )
                when
                    projects.registration_date is not null
                    then
                        abs(
                            (
                                projects.registration_date
                                - projects.date_start_installation
                            )
                        )
                else
                    abs(
                        (
                            projects.date_prereport
                            - projects.date_start_installation
                        )
                    )
            end
        ) as average_days_to_start_installation,
        avg(
            case
                when
                    projects.preregistration_date is not null
                    then
                        abs(
                            (
                                projects.preregistration_date
                                - projects.date_legal_docs
                            )
                        )
                when
                    projects.registration_date is not null
                    then
                        abs(
                            (
                                projects.registration_date
                                - projects.date_legal_docs
                            )
                        )
                else
                    abs(
                        (
                            projects.date_prereport
                            - projects.date_legal_docs
                        )
                    )
            end
        ) as average_days_to_legalize_installation,
        ((
            (
                count(*) filter (
                    where projects.status in (
                        'end installation',
                        'legal registration',
                        'legalization',
                        'warranty payment'
                    )
                )::decimal
                + count(*) filter (
                    where projects.status = 'discarded'
                )::decimal
            )
            / count(*)::decimal
        ) * 100) as percentage_executed_and_discarded
    from projects
    inner join campaigns on projects.campaign_id = campaigns.campaign_id
    group by campaigns.campaign_name
)

select * from campaign_summary
