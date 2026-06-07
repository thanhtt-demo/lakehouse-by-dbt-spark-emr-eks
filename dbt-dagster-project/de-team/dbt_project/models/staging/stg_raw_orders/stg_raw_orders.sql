-- ---------------------------------------------------------------------------------------------------------------------
-- STAGING MODEL: stg_raw_orders  (DATA-SKEW DEMO)
-- Self-contained synthetic generator — does NOT read raw.raw_orders, so no external/manual data
-- seeding is needed: deploy the code and run the job, that's it.
--
-- It generates `skew_num_orders` rows (dbt var, default 5,000,000) with a DELIBERATELY skewed
-- customer_id: ~95% of rows collapse onto a single hot key ('C000-HOT'). A downstream GROUP BY
-- customer_id (the `orders` mart) then routes ~95% of rows into ONE shuffle partition -> one
-- reduce task does almost all the work while the rest sit idle = classic data skew, which
-- DataFlint should flag on the Spark UI.
--
-- Tunable without code change:  dbt build ... --vars '{"skew_num_orders": 20000000}'
-- Materialization + Spark config declared in stg_raw_orders.yml. Maps to Glue database: staging.
-- ---------------------------------------------------------------------------------------------------------------------

{% set num_orders = var('skew_num_orders', 50000000) %}

with generated as (

    -- range(...) is a distributed table-valued function — generates ids across partitions
    -- without materializing a giant array on the driver (unlike sequence()/explode()).
    select id from range(0, {{ num_orders }})

),

skewed as (

    select
        concat('O', lpad(cast(id as string), 12, '0'))                              as order_id,
        -- Deterministic skew: 1 in 20 rows gets a spread-out id (C0..C999), the other
        -- ~95% all get the same hot key. pmod keeps it reproducible on every run.
        case
            when pmod(id, 20) = 0 then concat('C', cast(pmod(id, 1000) as string))
            else 'C000-HOT'
        end                                                                         as customer_id,
        date_add(date '2024-01-01', cast(pmod(id, 365) as int))                     as order_date,
        case pmod(id, 3)
            when 0 then 'completed'
            when 1 then 'pending'
            else 'cancelled'
        end                                                                         as status,
        round(rand() * 500, 2)                                                      as amount,
        'USD'                                                                       as currency,
        cast(date_add(date '2024-01-01', cast(pmod(id, 365) as int)) as timestamp)  as updated_at
    from generated

)

select
    *,
    {{ etl_at() }} as etl_at
from skewed
