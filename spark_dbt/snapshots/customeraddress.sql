{% snapshot customeraddress_snapshot %}

{{
    config(
        file_format = "delta",
        loaction_root = "/mnt/intermediate-data/customeraddress",
        target_schema = 'snapshots',
        invalidate_hard_deletes = True,
        unique_key = "CustomerID||'-'||AddressID",
        strategy='check',
        check_cols = 'all'
    )
}}

with customeraddress_data as (
    select 
        CustomerID,
        AddressID,
        AddressType
    from {{source('saleslt','customeraddress')}}
)

select * 
from customeraddress_data

{% endsnapshot %}