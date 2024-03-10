{% snapshot product_snapshot %}

{{
    config(
        file_format = "delta",
        loaction_root = "/mnt/intermediate-data/product",
        target_schema = 'snapshots',
        invalidate_hard_deletes = True,
        unique_key = 'ProductID',
        strategy='check',
        check_cols = 'all'
    )
}}

with product_data as (
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
)

select * 
from product_data

{% endsnapshot %}