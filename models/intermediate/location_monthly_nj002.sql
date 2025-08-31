-- models/intermediate/location_monthly_nj002.sql
-- Aggregiert die täglichen Verkaufsdaten für den Standort NJ_002 auf Monatsbasis.

with source_data as (

    select
        transaction_date,
        movie_id,
        ticket_amount,
        revenue
    from {{ ref('stg_nj_002') }}

)

select
    date_trunc('month', transaction_date)::date as month,
    'NJ_002' as location_id,
    movie_id,
    sum(ticket_amount) as total_tickets_sold,
    sum(revenue) as total_revenue
from source_data
group by 1, 2, 3
