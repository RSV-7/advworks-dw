{{
    config(
        unique_key='product_id', 
        strategy='check', 
        check_cols=['prod_name', 'vendor_id'] 
    )
}}

select
    pv.productid as product_id,
    pv.businessentityid as vendor_id,
    pr.name as prod_name,
    pr.productnumber as prod_nr,
    pv.averageleadtime as avg_lead_time,
    pv.standardprice as std_price,
    pv.lastreceiptcost as last_receipt_cost,
    pv.lastreceiptdate as last_receipt_date,
    pv.minorderqty as min_order_qty,
    pv.maxorderqty as max_order_qty,
    pv.onorderqty as on_order_qty,
    pv.unitmeasurecode as unit_measure_code,
    pr.makeflag as make_flag,
    pr.safetystocklevel as safety_stock_level,
    pr.reorderpoint as reorder_point,
    pr.standardcost as std_cost,
    pr.listprice as list_price,
    pv.modifieddate as last_update
from {{ source("purchasing", "productvendor") }} pv
    inner join {{ source("production", "product") }} pr on pr.productid = pv.productid