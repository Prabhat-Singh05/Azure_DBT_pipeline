{% snapshot customer_snapshot %}

{{
    config(
        file_format = "delta",
        loaction_root = "/mnt/intermediate-data/customer",
        target_schema = 'snapshots',
        invalidate_hard_deletes = True,
        unique_key = 'CustomerID',
        strategy='check',
        check_cols = 'all'
    )
}}

with customer_data as (
    select 
        CustomerID,
        NameStyle,
        Title,
        FirstName,
        MiddleName,
        LastName,
        Suffix,
        CompanyName,
        SalesPerson,
        EmailAddress,
        Phone,
        PasswordHash,
        PasswordSalt
    from {{source ('saleslt','customer')}}
)

select * 
from customer_data

{% endsnapshot %}