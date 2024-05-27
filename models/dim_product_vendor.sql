with base_data as (
    select
        product_id,
        vendor_id,
        prod_name,
        prod_nr,
        avg_lead_time,
        std_price,
        last_receipt_cost,
        last_receipt_date,
        min_order_qty,
        max_order_qty,
        on_order_qty,
        unit_measure_code,
        make_flag,
        safety_stock_level,
        reorder_point,
        std_cost,
        list_price,
        last_update as dbt_updated_at 
    from {{ ref("stg_product_vendor") }}

),
scd_data as (
    select
        row_number() over() as dbt_scd_id, 
        product_id,
        vendor_id,
        prod_name,
        prod_nr,
        avg_lead_time,
        std_price,
        last_receipt_cost,
        last_receipt_date,
        min_order_qty,
        max_order_qty,
        on_order_qty,
        unit_measure_code,
        make_flag,
        safety_stock_level,
        reorder_point,
        std_cost,
        list_price,
        dbt_updated_at,
        row_number() over(partition by product_id, vendor_id order by dbt_updated_at) as row_nr
    from base_data
)
select
    dbt_scd_id as sk_product_vendor,
    product_id,
    vendor_id,
    prod_name,
    prod_nr,
    avg_lead_time,
    std_price,
    last_receipt_cost,
    last_receipt_date,
    min_order_qty,
    max_order_qty,
    on_order_qty,
    unit_measure_code,
    make_flag,
    safety_stock_level,
    reorder_point,
    std_cost,
    list_price,
    case
        when row_nr = 1 then '1970-01-01'
        else dbt_updated_at
    end as valid_from,
    coalesce(null, '2200-01-01') as valid_to, 
    dbt_updated_at as last_updated_at
from scd_data
