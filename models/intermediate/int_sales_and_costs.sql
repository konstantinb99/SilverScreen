-- models/intermediate/int_sales_and_costs.sql
-- Verknüpft die vereinheitlichten Verkaufsdaten mit den aggregierten monatlichen Mietkosten.

with sales as (

    select * from {{ ref('int_locations_unioned') }}

),

costs as (

    select * from {{ ref('monthly_invoices') }}

),

final as (

    select
        sales.month,
        sales.location_id,
        sales.movie_id,
        sales.total_tickets_sold,
        sales.total_revenue,
        coalesce(costs.total_rental_cost, 0) as total_rental_cost -- Ersetzt NULL-Werte bei den Kosten durch 0

    from sales
    left join costs
        on sales.month = costs.month
        and sales.location_id = costs.location_id
        and sales.movie_id = costs.movie_id
)

select * from final
