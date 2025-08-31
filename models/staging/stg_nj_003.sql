-- models/staging/stg_nj_003.sql

-- Filtert, bereinigt und standardisiert Transaktionsdaten vom Standort NJ_003.
-- Es werden nur Ticketverkäufe berücksichtigt und die movie_id wird aus der 'details'-Spalte extrahiert.

WITH source AS (

    SELECT
        timestamp,
        transaction_id,
        product_type,
        details,
        amount,
        total_value
    FROM
        {{ source('silverscreen', 'nj_003') }}

)

SELECT
    -- Normalisiert den Zeitstempel auf den ersten Tag des Monats.
    DATE_TRUNC('MONTH', timestamp)::DATE AS month,
    -- Die movie_id wird aus der 'details'-Spalte extrahiert.
    details AS movie_id,
    -- Standardisiert Spaltennamen für die spätere Vereinigung.
    amount AS tickets_sold,
    total_value AS revenue
FROM
    source
WHERE
    -- Filtert nur nach Transaktionen, die Ticketverkäufe sind.
    product_type = 'ticket'
