-- models/staging/stg_nj_003.sql

with source_data as (

    select
        timestamp::date as transaction_date,
        transaction_id,
        product_type,
        details as movie_id, -- Movie ID ist in 'details'
        amount as ticket_amount,
        price as ticket_price,
        total_value as revenue
    from {{ source('silverscreen', 'nj_003') }}

)

select
    transaction_date,
    transaction_id,
    product_type,
    movie_id,
    ticket_amount,
    ticket_price,
    revenue
from source_data

