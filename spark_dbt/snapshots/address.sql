{% snapshot address_snapshot %}

{{
    config(
        file_format = "delta",
        loaction_root = "/mnt/intermediate-data/address", 
        target_schema = 'snapshots',
        invalidate_hard_deletes = True,
        unique_key = 'AddressID',
        strategy='check',
        check_cols = 'all'
    )
}}

with address_data as (
    select 
        AddressID,
        AddressLine1,
        City,
        StateProvince,
        countryRegion,
        PostalCode
    from {{source('saleslt','address')}}
)

select * 
from address_data

{% endsnapshot %}