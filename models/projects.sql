with

projects as (

    select * from {{ ref('stg_projects') }}

),

projects_summary as (
    select
        projects.project_id as project_id,
        projects.project_name as project_name,
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
                else abs((projects.date_prereport - projects.date_legal_docs))
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
                + count(*) filter (where projects.status = 'discarded')::decimal
            )
            / count(*)::decimal
        ) * 100) as percentage_executed_and_discarded
    from projects
)

select * from projects_summary
