{{
    config(
        unique_key='vendor_id',
        strategy='check', 
        check_cols=['account_number', 'vendor_name'] 
    )
}}

select
    businessentityid as vendor_id,
    accountnumber as account_number,
    name as vendor_name,    
    preferredvendorstatus as preferred_vendor_status,
    modifieddate as vendor_last_update
from {{ source("purchasing", "vendor") }}  

