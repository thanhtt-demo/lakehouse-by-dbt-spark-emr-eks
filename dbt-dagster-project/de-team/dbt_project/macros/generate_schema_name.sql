-- ---------------------------------------------------------------------------------------------------------------------
-- CUSTOM SCHEMA NAME GENERATION
-- Maps dbt custom schemas directly to Glue Data Catalog database names.
-- Without this macro, dbt would concatenate target schema + custom schema (e.g. "staging_staging").
-- With this macro:
--   staging models   → Glue database "staging"
--   intermediate     → Glue database "intermediate"
--   marts            → Glue database "marts"
-- ---------------------------------------------------------------------------------------------------------------------

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
