-- ---------------------------------------------------------------------------------------------------------------------
-- STAGING MODEL: stg_raw_customers
-- Source-conformed model — selects from raw customers source with minimal transformations.
-- Materialization + Spark config declared in stg_raw_customers.yml.
-- Maps to Glue Data Catalog database: staging
-- ---------------------------------------------------------------------------------------------------------------------

with source as (

    select * from {{ source('raw', 'raw_customers') }}

),

renamed as (

    select
        customer_id,
        customer_name,
        email,
        region,
        created_at,
        updated_at
    from source

)

select
    *,
    {{ etl_at() }} as etl_at
from renamed
