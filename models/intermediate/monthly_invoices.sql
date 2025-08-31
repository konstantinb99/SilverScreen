-- models/intermediate/monthly_invoices.sql
-- Aggregiert die Rechnungsdaten, um die gesamten monatlichen Mietkosten pro Film und Standort zu erhalten.

with source_data as (

    select
        invoice_month,
        location_id,
        movie_id,
        rental_cost
    from {{ ref('stg_invoices') }}

)

select
    invoice_month as month,
    location_id,
    movie_id,
    sum(rental_cost) as total_rental_cost
from source_data
group by 1, 2, 3
