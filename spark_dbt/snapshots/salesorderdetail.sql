{% snapshot salesorderdetail_snapshot %}

{{
    config(
        file_format = "delta",
        loaction_root = "/mnt/intermediate-data/salesorderdetail",
        target_schema = 'snapshots',
        invalidate_hard_deletes = True,
        unique_key = 'SalesOrderDetailID',
        strategy='check',
        check_cols = 'all'
    )
}}

with salesorderdetail_data as (
    select 
        SalesOrderID,
        SalesOrderDetailID,
        OrderQty,
        ProductID,
        UnitPrice,
        UnitPriceDiscount,
        LineTotal
    from {{source('saleslt','salesorderdetail')}}
)

select * 
from salesorderdetail_data

{% endsnapshot %}