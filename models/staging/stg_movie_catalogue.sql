-- models/staging/stg_movie_catalogue.sql

with source_data as (

    select
        movie_id,
        movie_title,
        release_date::date as release_date,
        genre,
        country,
        studio,
        budget,
        director,
        rating,
        minutes
    from {{ source('silverscreen', 'movie_catalogue') }}

)

select
    movie_id,
    movie_title,
    release_date,
    coalesce(genre, 'Unknown') as genre, -- Ersetzt NULL-Werte bei Genre
    country,
    studio,
    budget,
    director,
    rating,
    minutes
from source_data

