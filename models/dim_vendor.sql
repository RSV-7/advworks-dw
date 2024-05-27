with base_data as (
    select
        vendor_id,
        account_number,
        vendor_name,
        preferred_vendor_status,
        vendor_last_update as dbt_updated_at 
    from {{ ref("stg_vendor") }}
),
scd_data as (
    select
        row_number() over() as dbt_scd_id, 
        vendor_id,
        account_number,
        vendor_name,
        preferred_vendor_status,
        dbt_updated_at,
        row_number() over(partition by vendor_id order by dbt_updated_at) as row_nr
    from base_data
)
select
    dbt_scd_id as sk_vendor,
    vendor_id,
    account_number,
    vendor_name,
    preferred_vendor_status,
    case
        when row_nr = 1 then '1970-01-01'
        else dbt_updated_at
    end as valid_from,
    coalesce(null, '2200-01-01') as valid_to, 
    dbt_updated_at as last_updated_at
from scd_data
