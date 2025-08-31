-- models/staging/stg_invoices.sql

-- Bereitet Rechnungsdaten für die Analyse vor.
-- Duplikate basierend auf invoice_id werden entfernt.
-- Anschließend werden die Kosten pro Film, Monat und Standort aggregiert,
-- um die Granularität an das finale Modell anzupassen und "Fan-Out"-Joins zu verhindern.

WITH source AS (

    SELECT
        invoice_id,
        movie_id,
        month,
        location_id,
        total_invoice_sum
    FROM
        {{ source('silverscreen', 'invoices') }}

),

deduplicated AS (
    SELECT
        *,
        -- Weist jeder Zeile innerhalb einer Gruppe von identischen invoice_ids eine eindeutige Nummer zu.
        ROW_NUMBER() OVER(PARTITION BY invoice_id ORDER BY month DESC) as rn
    FROM source
),

-- Wählt nur die erste Zeile für jede invoice_id aus und entfernt so die Rohdaten-Duplikate.
cleaned AS (
    SELECT
        movie_id,
        location_id,
        DATE_TRUNC('MONTH', month)::DATE AS month,
        total_invoice_sum AS rental_cost
    FROM
        deduplicated
    WHERE rn = 1
)

-- Aggregiert die bereinigten Daten, um eine eindeutige Zeile pro Film, Standort und Monat zu gewährleisten.
SELECT
    movie_id,
    location_id,
    month,
    SUM(rental_cost) AS rental_cost
FROM
    cleaned
GROUP BY
    1, 2, 3

