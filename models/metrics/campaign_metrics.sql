-- TODO: to rethink this query

with

stg_projects as (

    select * from {{ ref('stg_projects') }}

),

projects_summary as (
    select
        stg_projects.project_id as project_id,
        stg_projects.project_name as project_name,
        avg(
            case
                when
                    stg_projects.preregistration_date is not null
                    then
                        abs(
                            (
                                stg_projects.preregistration_date
                                - stg_projects.date_start_installation
                            )
                        )
                when
                    stg_projects.registration_date is not null
                    then
                        abs(
                            (
                                stg_projects.registration_date
                                - stg_projects.date_start_installation
                            )
                        )
                else
                    abs(
                        (
                            stg_projects.date_prereport
                            - stg_projects.date_start_installation
                        )
                    )
            end
        ) as average_days_to_start_installation,
        avg(
            case
                when
                    stg_projects.preregistration_date is not null
                    then
                        abs(
                            (
                                stg_projects.preregistration_date
                                - stg_projects.date_legal_docs
                            )
                        )
                when
                    stg_projects.registration_date is not null
                    then
                        abs(
                            (
                                stg_projects.registration_date
                                - stg_projects.date_legal_docs
                            )
                        )
                else
                    abs(
                        (
                            stg_projects.date_prereport
                            - stg_projects.date_legal_docs
                        )
                    )
            end
        ) as average_days_to_legalize_installation,
        ((
            (
                count(*) filter (
                    where stg_projects.status in (
                        'end installation',
                        'legal registration',
                        'legalization',
                        'warranty payment'
                    )
                )::decimal
                + count(*) filter (
                    where stg_projects.status = 'discarded'
                )::decimal
            )
            / count(*)::decimal
        ) * 100) as percentage_executed_and_discarded
    from stg_projects
    group by project_id, project_name
)

select * from projects_summary
