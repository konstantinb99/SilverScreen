-- models/intermediate/location_monthly_nj003.sql
-- Filtert Ticketverkäufe und aggregiert die Daten für den Standort NJ_003 auf Monatsbasis.

with source_data as (

    select
        transaction_date,
        movie_id,
        ticket_amount,
        revenue,
        product_type
    from {{ ref('stg_nj_003') }}

)

select
    date_trunc('month', transaction_date)::date as month,
    'NJ_003' as location_id,
    movie_id,
    sum(ticket_amount) as total_tickets_sold,
    sum(revenue) as total_revenue
from source_data
where lower(product_type) = 'ticket' -- Stellt sicher, dass nur Ticketverkäufe gezählt werden
group by 1, 2, 3
