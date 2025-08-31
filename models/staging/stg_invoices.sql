-- models/staging/stg_invoices.sql

-- Bereitet Rechnungsdaten für die Analyse vor.
-- Das Monat wird auf den ersten Tag des Monats normalisiert und Spalten werden für Klarheit umbenannt.
-- Duplikate basierend auf invoice_id werden entfernt.

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
)

SELECT
    invoice_id,
    movie_id,
    location_id,
    -- Normalisiert das Datum auf den ersten Tag des Monats für konsistente Joins.
    DATE_TRUNC('MONTH', month)::DATE AS month,
    -- Benennt total_invoice_sum in rental_cost um, um die Bedeutung zu verdeutlichen.
    total_invoice_sum AS rental_cost
FROM
    deduplicated
-- Wählt nur die erste Zeile für jede invoice_id aus und entfernt so die Duplikate.
WHERE rn = 1

