-- models/staging/stg_nj_002.sql

with source_data as (

    select
        "date"::date as "transaction_date",
        "movie_id",
        "ticket_amount",
        "ticket_price",
        "total_earned" as "revenue"
    from {{ source('silverscreen', 'NJ_002') }}

)

select
    "transaction_date",
    "movie_id",
    "ticket_amount",
    "ticket_price",
    "revenue"
from source_data


