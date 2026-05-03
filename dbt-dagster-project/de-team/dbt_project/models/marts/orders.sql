-- ---------------------------------------------------------------------------------------------------------------------
-- MARTS MODEL: orders
-- Business-facing orders table with enriched data from staging.
-- Uses incremental materialization with merge strategy for Iceberg ACID upserts.
-- Maps to Glue Data Catalog database: marts
-- ---------------------------------------------------------------------------------------------------------------------

{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        file_format='iceberg',
        unique_key='order_id'
    )
}}

with stg_orders as (

    select * from {{ ref('stg_raw_orders') }}

)

select
    order_id,
    customer_id,
    order_date,
    status,
    amount,
    currency,
    updated_at,
    current_timestamp() as dbt_updated_at

from stg_orders

{% if is_incremental() %}
where updated_at > (select max(updated_at) from {{ this }})
{% endif %}
