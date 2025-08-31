-- models/marts/fct_monthly_movie_performance.sql

-- Erstellt die finale Faktentabelle durch die Verknüpfung von vereinheitlichten Standortdaten,
-- Rechnungsinformationen (Mietkosten) und Filmdetails.
-- Das Ergebnis ist eine umfassende Tabelle der monatlichen Filmleistung pro Standort.

WITH locations_unioned AS (
    SELECT * FROM {{ ref('int_locations_unioned') }}
),

invoices AS (
    SELECT * FROM {{ ref('stg_invoices') }}
),

movie_catalogue AS (
    SELECT
        movie_id,
        movie_title,
        genre,
        studio
    FROM {{ ref('stg_movie_catalogue') }}
)

SELECT
    loc.movie_id,
    cat.movie_title,
    cat.genre,
    cat.studio,
    loc.month,
    loc.location,
    -- Stellt sicher, dass Mietkosten 0 sind, wenn keine Rechnung gefunden wird.
    COALESCE(inv.rental_cost, 0) AS rental_cost,
    loc.tickets_sold,
    loc.revenue
FROM
    locations_unioned loc
LEFT JOIN
    invoices inv ON loc.movie_id = inv.movie_id
    AND loc.month = inv.month
    AND loc.location = inv.location_id
LEFT JOIN
    movie_catalogue cat ON loc.movie_id = cat.movie_id
