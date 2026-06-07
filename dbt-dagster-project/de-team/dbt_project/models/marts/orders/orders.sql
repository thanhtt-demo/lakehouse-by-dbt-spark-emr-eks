-- ---------------------------------------------------------------------------------------------------------------------
-- MARTS MODEL: orders  (DATA-SKEW DEMO)
-- Goal: a shuffle that is genuinely skewed by the hot customer_id, so DataFlint flags a
-- long-tail / partition-skew task.
--
-- WHY A PLAIN GROUP BY DID NOT SKEW:
--   count()/sum()/avg() are partial-aggregable, so Spark does a map-side combine BEFORE the
--   shuffle — every map task collapses all its 'C000-HOT' rows into a SINGLE partial row. The
--   reduce side then receives only ~(#map partitions) partial rows for the hot key, not 95% of
--   the data. Result: no reduce-side skew, fast regardless of row count (even 200M).
--
-- WHAT ACTUALLY SKEWS:
--   A window function CANNOT be partial-aggregated — Spark must ship EVERY row of a partition to
--   ONE task and sort it there. Partitioning the window by the skewed customer_id therefore drags
--   ~95% of all rows onto a single task (a big on-task sort that spills) = real, unavoidable skew.
--   The `ranked` CTE below is the skewed stage; we then aggregate down to one row per customer so
--   the OUTPUT stays small (the heavy work is the skewed window shuffle, not the write).
--
-- AQE is disabled in orders.yml so the skew is not auto-mitigated. Maps to Glue database: marts.
-- ---------------------------------------------------------------------------------------------------------------------

with stg_orders as (

    select * from {{ ref('stg_raw_orders') }}

),

ranked as (

    -- Skewed stage: PARTITION BY the hot key + an ORDER BY forces a per-partition sort on one
    -- task for 'C000-HOT'. This is what makes the job slow on a single task = the skew signal.
    select
        customer_id,
        amount,
        order_date,
        row_number() over (partition by customer_id order by order_date, amount) as order_seq
    from stg_orders

)

select
    customer_id,
    count(*)              as order_count,
    sum(amount)           as total_amount,
    avg(amount)           as avg_amount,
    -- Reference the window result so the optimizer cannot prune the (skewed) window stage.
    max(order_seq)        as max_order_seq,
    current_timestamp()   as dbt_updated_at,
    {{ etl_at() }}        as etl_at

from ranked
group by customer_id
