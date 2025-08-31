-- models/staging/stg_nj_001.sql

with source_data as (

    select
        "timestamp"::date as "transaction_date",
        "transaction_id",
        "movie_id",
        "ticket_amount",
        "price" as "ticket_price",
        "transaction_total" as "revenue",
        "is_discounted",
        "is_3D"
    from {{ source('silverscreen', 'NJ_001') }}

)

select
    "transaction_date",
    "transaction_id",
    "movie_id",
    "ticket_amount",
    "ticket_price",
    "revenue",
    "is_discounted",
    "is_3D"
from source_data

