-- models/staging/stg_nj_001.sql

-- Bereinigt und standardisiert die Transaktionsdaten vom Standort NJ_001.
-- Dies dient als Basis für die monatliche Aggregation.

WITH source AS (

    SELECT
        timestamp,
        transaction_id,
        movie_id,
        ticket_amount,
        transaction_total
    FROM
        {{ source('silverscreen', 'nj_001') }}

)

SELECT
    -- Normalisiert den Zeitstempel auf den ersten Tag des Monats.
    DATE_TRUNC('MONTH', timestamp)::DATE AS month,
    movie_id,
    -- Standardisiert Spaltennamen für die spätere Vereinigung.
    ticket_amount AS tickets_sold,
    transaction_total AS revenue
FROM
    source
