{{
    config(
        materialized = "table",
        file_format = "delta",
        loaction_root = "mnt/final-data/customer"
    )
}}

with salesorderdeatail_data as (
    select 
        SalesOrderID,
        SalesOrderDetailID,
        OrderQty,
        ProductID,
        UnitPrice,
        UnitPriceDiscount,
        LineTotal
    from {{ ref ('salesorderdetail_snapshot') }} where dbt_valid_to is null
),

product_data as (
    select 
        ProductID,
        Name,
        ProductNumber,
        Color,
        StandardCost,
        ListPrice,
        Size,
        Weight,
        ProductCategoryID,
        ProductModelID,
        SellStartDate,
        SellEndDate,
        DiscontinuedDate,
        ThumbNailPhoto,
        ThumbnailPhotoFileName
    from {{source('saleslt','product')}}
),

salesorderheader_data as (
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
),


transformed as (
    select
        salesorderdeatail_data.SalesOrderID,
        salesorderdeatail_data.SalesOrderDetailID,
        salesorderdeatail_data.OrderQty,
        salesorderdeatail_data.UnitPrice,
        salesorderdeatail_data.UnitPriceDiscount,
        salesorderdeatail_data.LineTotal,
        product_data.ProductID,
        product_data.Name,
        product_data.ProductNumber,
        product_data.Color,
        product_data.StandardCost,
        product_data.ListPrice,
        product_data.Size,
        product_data.Weight,
        product_data.ProductCategoryID,
        product_data.ProductModelID,
        product_data.SellStartDate,
        product_data.SellEndDate,
        product_data.DiscontinuedDate,
        product_data.ThumbNailPhoto,
        product_data.ThumbnailPhotoFileName,
        salesorderheader_data.RevisionNumber,
        salesorderheader_data.OrderDate,
        salesorderheader_data.DueDate,
        salesorderheader_data.ShipDate,
        salesorderheader_data.Status,
        salesorderheader_data.OnlineOrderFlag,
        salesorderheader_data.SalesOrderNumber,
        salesorderheader_data.PurchaseOrderNumber,
        salesorderheader_data.AccountNumber,
        salesorderheader_data.CustomerID,
        salesorderheader_data.ShipToAddressID,
        salesorderheader_data.BillToAddressID,
        salesorderheader_data.ShipMethod,
        salesorderheader_data.CreditCardApprovalCode,
        salesorderheader_data.SubTotal,
        salesorderheader_data.TaxAmt,
        salesorderheader_data.Freight,
        salesorderheader_data.TotalDue,
        salesorderheader_data.Comment
    from salesorderdeatail_data 
    left join product_data on salesorderdeatail_data.ProductID = product_data.ProductID
    left join salesorderheader_data on salesorderdeatail_data.SalesOrderID = salesorderheader_data.SalesOrderID
)

select *
from transformed 
