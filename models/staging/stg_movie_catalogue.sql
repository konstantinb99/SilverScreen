-- models/staging/stg_movie_catalogue.sql

-- Wählt Daten aus der Quelltabelle movie_catalogue aus und führt grundlegende Bereinigungen durch.
-- Fehlende Genre-Werte werden durch 'Unknown' ersetzt, um Datenkonsistenz zu gewährleisten.
-- Duplikate basierend auf movie_id werden entfernt, um "Fan-Out"-Joins zu verhindern.

WITH source AS (

    SELECT
        movie_id,
        movie_title,
        release_date,
        genre,
        country,
        studio,
        budget,
        director,
        rating,
        minutes
    FROM
        {{ source('silverscreen', 'movie_catalogue') }}

),

deduplicated AS (
    SELECT
        *,
        -- Weist jeder Zeile innerhalb einer Gruppe von identischen movie_ids eine eindeutige Nummer zu.
        ROW_NUMBER() OVER(PARTITION BY movie_id ORDER BY release_date DESC) as rn
    FROM
        source
)

SELECT
    movie_id,
    movie_title,
    release_date,
    -- Wenn das Genre NULL ist, wird es durch 'Unknown' ersetzt.
    COALESCE(genre, 'Unknown') AS genre,
    country,
    studio,
    budget,
    director,
    rating,
    minutes
FROM
    deduplicated
-- Wählt nur die erste Zeile für jede movie_id aus und entfernt so die Duplikate.
WHERE
    rn = 1

