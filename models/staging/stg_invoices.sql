-- models/staging/stg_invoices.sql

with source_data as (

    select
        movie_id,
        invoice_id,
        month::date as invoice_month,
        location_id,
        studio, 
        release_date::date as release_date, 
        weekly_price, 
        total_invoice_sum as rental_cost
    from {{ source('silverscreen', 'invoices') }}

)

select
    movie_id,
    invoice_id,
    invoice_month,
    location_id,
    studio,
    release_date,
    weekly_price,
    rental_cost
from source_data

