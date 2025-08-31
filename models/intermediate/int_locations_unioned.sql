-- models/intermediate/int_locations_unioned.sql

-- Aggregiert die Daten von jedem Standort auf monatlicher Basis und vereinigt sie in einer Tabelle.
-- Dies schafft eine einheitliche Sicht auf Ticketverkäufe und Einnahmen über alle Standorte hinweg.

WITH nj_001_monthly AS (
    SELECT
        'NJ_001' AS location,
        month,
        movie_id,
        SUM(tickets_sold) AS tickets_sold,
        SUM(revenue) AS revenue
    FROM
        {{ ref('stg_nj_001') }}
    GROUP BY
        1, 2, 3
),

nj_002_monthly AS (
    SELECT
        'NJ_002' AS location,
        month,
        movie_id,
        SUM(tickets_sold) AS tickets_sold,
        SUM(revenue) AS revenue
    FROM
        {{ ref('stg_nj_002') }}
    GROUP BY
        1, 2, 3
),

nj_003_monthly AS (
    SELECT
        'NJ_003' AS location,
        month,
        movie_id,
        SUM(tickets_sold) AS tickets_sold,
        SUM(revenue) AS revenue
    FROM
        {{ ref('stg_nj_003') }}
    GROUP BY
        1, 2, 3
)

-- Vereinigt die monatlichen Daten aller drei Standorte.
SELECT * FROM nj_001_monthly
UNION ALL
SELECT * FROM nj_002_monthly
UNION ALL
SELECT * FROM nj_003_monthly
