-- ---------------------------------------------------------------------------------------------------------------------
-- MARTS MODEL: orders  (DATA-SKEW DEMO)
-- Aggregates stg_raw_orders by customer_id. Because stg_raw_orders concentrates ~95% of rows on a
-- single hot customer_id, this GROUP BY produces a heavily skewed shuffle: one reduce task reads
-- ~95% of the data while the others read almost nothing. That imbalance is what DataFlint should
-- surface as a data-skew / long-tail-task alert on the Spark UI.
--
-- Materialized as a full `table` (not incremental) and AQE is disabled in orders.yml
-- (meta.spark_config.spark_properties) so Spark does NOT auto-mitigate the skew — otherwise there
-- would be nothing for DataFlint to detect.
-- Maps to Glue Data Catalog database: marts.
-- ---------------------------------------------------------------------------------------------------------------------

with stg_orders as (

    select * from {{ ref('stg_raw_orders') }}

)

select
    customer_id,
    count(*)                as order_count,
    sum(amount)             as total_amount,
    avg(amount)             as avg_amount,
    min(order_date)         as first_order_date,
    max(order_date)         as last_order_date,
    current_timestamp()     as dbt_updated_at,
    {{ etl_at() }}          as etl_at

from stg_orders
group by customer_id
