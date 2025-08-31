-- models/staging/stg_movie_catalogue.sql

-- Wählt Daten aus der Quelltabelle movie_catalogue aus und führt grundlegende Bereinigungen durch.
-- Fehlende Genre-Werte werden durch 'Unknown' ersetzt, um Datenkonsistenz zu gewährleisten.

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
    source
