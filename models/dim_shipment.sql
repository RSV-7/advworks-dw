with base_data as (
    select
        shipmethod_id,
        company_name,
        ship_base,
        ship_rate,
        shipmethod_last_update as dbt_updated_at 
    from {{ ref("stg_shipment") }}
),
scd_data as (
    select
        row_number() over() as dbt_scd_id, 
        shipmethod_id,
        company_name,
        ship_base,
        ship_rate,
        dbt_updated_at,
        row_number() over(partition by shipmethod_id order by dbt_updated_at) as row_nr
    from base_data
)
select
    dbt_scd_id as sk_shipmethod,
    shipmethod_id,
    company_name,
    ship_base,
    ship_rate,
    case
        when row_nr = 1 then '1970-01-01'
        else dbt_updated_at
    end as valid_from,
    coalesce(null, '2200-01-01') as valid_to, 
    dbt_updated_at as last_updated_at
from scd_data
