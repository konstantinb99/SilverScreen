-- models/intermediate/location_monthly_nj001.sql
-- Aggregiert die Transaktionsdaten für den Standort NJ_001 auf Monatsbasis.

with source_data as (

    select
        transaction_date,
        movie_id,
        ticket_amount,
        revenue
    from {{ ref('stg_nj_001') }}

)

select
    date_trunc('month', transaction_date)::date as month,
    'NJ_001' as location_id,
    movie_id,
    sum(ticket_amount) as total_tickets_sold,
    sum(revenue) as total_revenue
from source_data
group by 1, 2, 3
