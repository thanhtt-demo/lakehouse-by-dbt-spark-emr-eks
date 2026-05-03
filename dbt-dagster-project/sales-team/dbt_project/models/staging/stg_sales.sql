-- ---------------------------------------------------------------------------------------------------------------------
-- STAGING MODEL: stg_sales
-- Source-conformed model — selects from raw sales source with minimal transformations.
-- Materialized as table (default from dbt_project.yml).
-- Maps to Glue Data Catalog database: staging
-- Executed on Amazon Athena (not Spark).
-- ---------------------------------------------------------------------------------------------------------------------

with source as (

    select * from {{ source('raw', 'raw_sales') }}

),

renamed as (

    select
        sale_id,
        product_id,
        customer_id,
        sale_date,
        quantity,
        unit_price,
        total_amount,
        region,
        updated_at
    from source

)

select * from renamed
