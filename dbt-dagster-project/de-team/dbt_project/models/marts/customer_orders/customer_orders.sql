-- ---------------------------------------------------------------------------------------------------------------------
-- MARTS MODEL: customer_orders
-- Business-facing table joining orders with customer details.
-- Provides enriched order data with customer name and region.
-- Maps to Glue Data Catalog database: marts
-- ---------------------------------------------------------------------------------------------------------------------

with orders as (

    select * from {{ ref('stg_raw_orders') }}

),

customers as (

    select * from {{ ref('stg_raw_customers') }}

),

joined as (

    select
        o.order_id,
        o.customer_id,
        c.customer_name,
        c.email as customer_email,
        c.region as customer_region,
        o.order_date,
        o.status,
        o.amount,
        o.currency,
        o.updated_at
    from orders o
    left join customers c on o.customer_id = c.customer_id

)

select * from joined
