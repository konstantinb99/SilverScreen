-- models/intermediate/int_locations_unioned.sql
-- Vereinheitlicht die monatlich aggregierten Daten aller drei Standorte zu einer einzigen Tabelle.

with location_nj001 as (

    select * from {{ ref('location_monthly_nj001') }}

),

location_nj002 as (

    select * from {{ ref('location_monthly_nj002') }}

),

location_nj003 as (

    select * from {{ ref('location_monthly_nj003') }}

)

select * from location_nj001
union all
select * from location_nj002
union all
select * from location_nj003
