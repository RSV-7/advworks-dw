{{
    config(
        unique_key='purchase_detail_id' 
    )
}}

with ordercounts as (
    select
        purchaseorderid,
        count(*) as product_count
    from {{ source("purchasing", "purchaseorderdetail") }}
    group by purchaseorderid
)

select
    pod.purchaseorderdetailid as purchase_detail_id,
    pv.name as vendor_name,
    prod.name as product_name,
    pod.unitprice as unit_price,
    pod.orderqty as order_qty,
    pod.receivedqty as received_qty,
    pod.rejectedqty as rejected_qty,
    pod.purchaseorderid as purchase_order_id,
    e.loginid as employee_loginid,
    ship.name as shipment,
    poh.status as order_status,
    round(poh.taxamt / oc.product_count, 3) as tax_amt,
    round(poh.freight / oc.product_count, 3) as freight,
    poh.shipdate as ship_date,
    pod.duedate as due_date,
    poh.orderdate as order_date,
    greatest(pod.modifieddate,poh.modifieddate) as purchasing_last_update
from {{ source("purchasing", "purchaseorderdetail") }} as pod
left join {{ source("purchasing", "purchaseorderheader") }} as poh on pod.purchaseorderid = poh.purchaseorderid
left join {{ source("purchasing", "vendor") }} as pv on poh.vendorid = pv.businessentityid
left join {{ source("purchasing", "shipmethod") }} as ship on poh.shipmethodid = ship.shipmethodid
left join {{ source("humanresources", "employee") }} as e on poh.employeeid = e.businessentityid
left join {{ source("production", "product") }} as prod on pod.productid = prod.productid
left join ordercounts as oc on pod.purchaseorderid = oc.purchaseorderid