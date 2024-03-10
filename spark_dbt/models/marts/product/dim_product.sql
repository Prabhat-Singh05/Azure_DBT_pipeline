{{
    config(
        materialized = "table",
        file_format = "delta",
        loaction_root = "mnt/final-data/customer"
    )
}}

with product_data as (
    select 
        ProductID,
        Name as ProductName,
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
        DiscontinuedDate
    from {{ ref ('product_snapshot') }} where dbt_valid_to is null
),

productmodel_data as (
   select 
        ProductModelID,
        Name,
        CatalogDescription
    from {{ ref ('productmodel_snapshot')}} where dbt_valid_to is null
),


transformed as (
    select
        row_number() over( order by product_data.ProductID) as product_sn,
        product_data.ProductID,
        product_data.ProductName,
        product_data.ProductNumber,
        product_data.StandardCost,
        product_data.ListPrice,
        product_data.ProductCategoryID,
        product_data.ProductModelID,
        product_data.SellStartDate,
        product_data.SellEndDate,
        product_data.DiscontinuedDate,
        productmodel_data.Name as Model_name,
        productmodel_data.CatalogDescription as Description
    from product_data 
    left join productmodel_data on product_data.ProductModelID = productmodel_data.ProductModelID
)

select *
from transformed 
