-- ---------------------------------------------------------------------------------------------------------------------
-- STAGING MODEL: stg_raw_orders
-- Source-conformed model — selects from raw orders source with minimal transformations.
-- Materialization + Spark config declared in stg_raw_orders.yml.
-- Maps to Glue Data Catalog database: staging
-- ---------------------------------------------------------------------------------------------------------------------

with source as (

    select * from {{ source('raw', 'raw_orders') }}

),

renamed as (

    select
        order_id,
        customer_id,
        order_date,
        status,
        amount,
        currency,
        updated_at
    from source

)

select
    *,
    {{ etl_at() }} as etl_at
from renamed
