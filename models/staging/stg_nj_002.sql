-- models/staging/stg_nj_002.sql

-- Bereinigt und standardisiert die täglichen Verkaufsdaten vom Standort NJ_002.
-- Dies dient als Basis für die monatliche Aggregation.

WITH source AS (

    SELECT
        date,
        movie_id,
        ticket_amount,
        total_earned
    FROM
        {{ source('silverscreen', 'nj_002') }}

)

SELECT
    -- Normalisiert das Datum auf den ersten Tag des Monats.
    DATE_TRUNC('MONTH', date)::DATE AS month,
    movie_id,
    -- Standardisiert Spaltennamen für die spätere Vereinigung.
    ticket_amount AS tickets_sold,
    total_earned AS revenue
FROM
    source
