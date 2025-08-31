-- models/marts/fct_monthly_movie_performance.sql
-- Finales Modell, das alle Daten zusammenführt, um die monatliche Performance pro Film und Standort darzustellen.

with sales_and_costs as (

    select * from {{ ref('int_sales_and_costs') }}

),

movie_catalogue as (

    select * from {{ ref('stg_movie_catalogue') }}

),

final as (

    select
        sales_and_costs.movie_id,
        coalesce(movie_catalogue.movie_title, 'Unknown') as movie_title,
        coalesce(movie_catalogue.genre, 'Unknown') as genre,
        coalesce(movie_catalogue.studio, 'Unknown') as studio,
        sales_and_costs.month,
        sales_and_costs.location_id as location,
        sales_and_costs.total_rental_cost as rental_cost,
        sales_and_costs.total_tickets_sold as tickets_sold,
        sales_and_costs.total_revenue as revenue

    from sales_and_costs
    left join movie_catalogue
        on sales_and_costs.movie_id = movie_catalogue.movie_id
)

select * from final
