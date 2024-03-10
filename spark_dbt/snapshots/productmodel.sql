{% snapshot productmodel_snapshot %}

{{
    config(
        file_format = "delta",
        loaction_root = "/mnt/intermediate-data/productmodel",
        target_schema = 'snapshots',
        invalidate_hard_deletes = True,
        unique_key = 'ProductModelID',
        strategy='check',
        check_cols = 'all'
    )
}}

with productmodel_data as (
    select 
        ProductModelID,
        Name,
        CatalogDescription
    from {{source('saleslt','productmodel')}}
)

select * 
from productmodel_data

{% endsnapshot %}