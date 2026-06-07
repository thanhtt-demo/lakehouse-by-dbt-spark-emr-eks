{#-
  -- etl_at(): ETL load timestamp in Vietnam time, for data lineage / tracing.
  --
  -- The Spark session timezone is set to Asia/Ho_Chi_Minh (spark.sql.session.timeZone in
  -- DEFAULT_SPARK_PROPERTIES), so current_timestamp() already returns the correct Vietnam
  -- instant and renders as Vietnam local time. No from_utc/to_utc conversion is needed —
  -- doing so here would double-shift the value.
  --
  -- Use as:  {{ etl_at() }} as etl_at
-#}
{% macro etl_at() %}
    current_timestamp()
{% endmacro %}
