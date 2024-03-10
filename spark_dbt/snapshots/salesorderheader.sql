{% snapshot salesorderheader_snapshot %}

{{
    config(
        file_format = "delta",
        loaction_root = "/mnt/intermediate-data/salesorderheader",
        target_schema = 'snapshots',
        invalidate_hard_deletes = True,
        unique_key = 'SalesOrderID',
        strategy='check',
        check_cols = 'all'
    )
}}

with salesorderheader_data as (
    select 
        SalesOrderID,
        RevisionNumber,
        OrderDate,
        DueDate,
        ShipDate,
        Status,
        OnlineOrderFlag,
        SalesOrderNumber,
        PurchaseOrderNumber,
        AccountNumber,
        CustomerID,
        ShipToAddressID,
        BillToAddressID,
        ShipMethod,
        CreditCardApprovalCode,
        SubTotal,
        TaxAmt,
        Freight,
        TotalDue,
        Comment
    from {{source('saleslt','salesorderheader')}}
)

select * 
from salesorderheader_data

{% endsnapshot %}