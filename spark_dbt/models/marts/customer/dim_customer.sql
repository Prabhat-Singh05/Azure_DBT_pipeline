{{
    config(
        materialized = "table",
        file_format = "delta",
        loaction_root = "mnt/final-data/customer"
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
    from {{ ref ('address_snapshot') }} where dbt_valid_to is null
),

customeraddress_data as (
    select 
            CustomerID,
            AddressID,
            AddressType
    from {{ ref ('customeraddress_snapshot')}} where dbt_valid_to is null
),

customer_data as (
    select 
        CustomerID,
        concat(ifnull(FirstName,' '), ' ',ifnull(MiddleName,' '),' ',ifnull(LastName,' ')) as fullname
    from {{ref ('customer_snapshot')}} where dbt_valid_to is null
),

transformed as (
    select
        row_number() over( order by customer_data.CustomerID) as customer_sn,
        customer_data.CustomerID,
        customer_data.fullname,
        customeraddress_data.AddressID,
        customeraddress_data.AddressType,
        address_data.AddressLine1,
        address_data.City,
        address_data.StateProvince,
        address_data.countryRegion,
        address_data.PostalCode
    from customer_data 
    join customeraddress_data on customer_data.CustomerID = customeraddress_data.CustomerID
    join address_data on customeraddress_data.AddressID = address_data.AddressID

)

select *
from transformed 
