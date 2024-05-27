{{
    config(
        unique_key='shipmethod_id', 
        strategy='check', 
        check_cols=['company_name', 'ship_base', 'ship_rate']
    )
}}

select
    shipmethodid as shipmethod_id,
    name as company_name,
    shipbase as ship_base,
    shiprate as ship_rate,
    modifieddate as shipmethod_last_update
from {{ source("purchasing", "shipmethod") }} 
