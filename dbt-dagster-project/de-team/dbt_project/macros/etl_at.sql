{#-
  -- etl_at(): current timestamp in Vietnam time (Asia/Ho_Chi_Minh), for data lineage / tracing.
  --
  -- Session-timezone independent: current_timestamp() is the "now" instant rendered in the Spark
  -- session timezone (spark.sql.session.timeZone). We normalize it to true UTC using
  -- current_timezone() (the session tz), then shift to Asia/Ho_Chi_Minh. This yields correct
  -- Vietnam wall-clock time regardless of how the Spark session timezone is configured.
  --
  -- Use as:  {{ etl_at() }} as etl_at
-#}
{% macro etl_at() %}
    from_utc_timestamp(to_utc_timestamp(current_timestamp(), current_timezone()), 'Asia/Ho_Chi_Minh')
{% endmacro %}
