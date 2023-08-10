{{ config(materialized='view') }}

{% set campaign_id = 66 %}


-- CREATE OR REPLACE VIEW campaign_metrics AS
SELECT
    campaign_id,
    COUNT(*) FILTER (WHERE status IN ('end installation', 'legal registration', 'legalization', 'warranty payment')) AS total_executed,
    COUNT(*) FILTER (WHERE status = 'discarded') AS discarded,
    COUNT(*) AS inscriptions,
    COUNT(*) FILTER (WHERE status = 'discarded' AND discarded_type = 'technical') AS discarded_technical,
    COUNT(*) FILTER (WHERE status = 'discarded' AND discarded_type = 'voluntary') AS discarded_voluntary,
    COUNT(*) FILTER (WHERE status = 'discarded' AND discarded_type = 'voluntary' AND date_report IS NULL) AS discarded_voluntary_before_technical_visit,
    COUNT(*) FILTER (WHERE status = 'discarded' AND discarded_type = 'voluntary' AND date_report IS NOT NULL) AS discarded_voluntary_after_technical_visit,
    COUNT(*) FILTER (WHERE status = 'registered') AS pending_prereport,
    COUNT(*) FILTER (WHERE status = 'prereport') AS pending_technical_visit,
    COUNT(*) FILTER (WHERE status = 'technical visit') AS pending_technical_scheduled_visit,
    COUNT(*) FILTER (WHERE status IN (
        'report', 'report review',
        'offer', 'offer review',
        'signature',
        'construction',
        'date installation set',
        'end installation',
        'legal registration',
        'legalization',
        'warranty payment'
    )) + COUNT(*) FILTER (WHERE status = 'discarded' AND discarded_type = 'technical') AS total_technical_visits_done,
    COUNT(*) FILTER (WHERE status IN ('report', 'report review')) AS technical_visit_report,
    COUNT(*) FILTER (WHERE status IN ('offer', 'offer review')) AS offers_sent,
    COUNT(*) FILTER (WHERE status IN (
        'signature',
        'construction',
        'date installation set',
        'end installation',
        'legal registration',
        'legalization',
        'warranty payment'
    )) AS total_signed_contracts,
    COUNT(*) FILTER (WHERE status = 'signature') AS signed_contracts,
    COUNT(*) FILTER (WHERE status = 'construction permit') AS pending_license_response,
    COUNT(*) FILTER (WHERE status = 'date installation set') AS scheduled_works,
    COUNT(*) FILTER (WHERE status = 'end installation') AS works_completed,
    COUNT(*) FILTER (WHERE status = 'legal registration') AS legalization_stage,
    COUNT(*) FILTER (WHERE status = 'legalization') AS legalization_completed,
    COUNT(*) FILTER (WHERE status IN (
        'end installation',
        'legal registration',
        'legalization',
        'warranty payment'
    )) AS total_executed_works,
    COUNT(*) FILTER (WHERE status = 'warranty payment') AS warranty_payment_done,
    AVG(
        CASE
            WHEN preregistration_date IS NOT NULL THEN ABS((preregistration_date - date_start_installation))
            WHEN registration_date IS NOT NULL THEN ABS((registration_date - date_start_installation))
            ELSE ABS((date_prereport - date_start_installation))
        END
    ) AS average_days_to_start_installation,
    AVG(
        CASE
            WHEN preregistration_date IS NOT NULL THEN ABS((preregistration_date - date_legal_docs))
            WHEN registration_date IS NOT NULL THEN ABS((registration_date - date_legal_docs))
            ELSE ABS((date_prereport - date_legal_docs))
        END
    ) AS average_days_to_legalize_installation,
    ((
        (COUNT(*) FILTER (WHERE status IN ('end installation', 'legal registration', 'legalization', 'warranty payment'))::decimal +
        COUNT(*) FILTER (WHERE status = 'discarded')::decimal) /
        COUNT(*)::decimal) * 100) AS percentage_executed_and_discarded
FROM {{ source('somsolet', 'somsolet_project')}}
-- WHERE campaign_id = {{ campaign_id }}
GROUP BY campaign_id
