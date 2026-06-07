{#-
  -- OVERRIDE: spark__list_relations_without_caching (Iceberg + Glue fix)
  --
  -- Problem: this project uses Iceberg's SparkSessionCatalog (spark_catalog) over AWS Glue.
  -- Iceberg tables registered in Glue have a null Hive StorageDescriptor.InputFormat. dbt-spark's
  -- default relation listing runs `show table extended in <schema> like '*'`, which goes through
  -- the Hive v1 path and fails with:
  --     HiveException: StorageDescriptor#InputFormat cannot be null for table: <t>
  --
  -- dbt-spark DOES have an Iceberg-safe listing path (show tables + describe extended per table,
  -- which never reads the Hive StorageDescriptor). But its SparkAdapter.list_relations_without_caching
  -- only switches to that path when the error message contains
  --     "SHOW TABLE EXTENDED is not supported for v2 tables"   (SPARK-33393, true v2 catalogs).
  -- With SparkSessionCatalog the error is the Glue "InputFormat cannot be null" message instead,
  -- which dbt-spark does NOT recognize -> it returns an EMPTY relation list. Consequences:
  --   - `table` models: harmless (they drop+create regardless).
  --   - `incremental` models (e.g. orders): dbt thinks the table doesn't exist -> full rebuild
  --     instead of an Iceberg MERGE, silently losing incremental behavior.
  --
  -- Fix: raise the exact v2 message so the Python adapter routes to its built-in Iceberg-safe
  -- listing (`list_relations_show_tables_without_caching` + `describe_table_extended_without_caching`).
  -- raise_compiler_error raises a CompilationError, which subclasses DbtRuntimeError and carries
  -- `.msg`, so it is caught and matched by the adapter's `elif "SHOW TABLE EXTENDED is not
  -- supported for v2 tables" in errmsg` branch.
  --
  -- Applies to BOTH submit backends (eks_client and emr_containers) since both use dbt-spark with
  -- the same catalog. The describe-per-table path is slightly slower for very large schemas, which
  -- is fine here.
  --
  -- NOTE: verify on a real cluster that `orders` runs an incremental MERGE (not a full rebuild)
  -- on its second run after this change lands.
-#}
{% macro spark__list_relations_without_caching(relation) %}
  {% do exceptions.raise_compiler_error("SHOW TABLE EXTENDED is not supported for v2 tables") %}
{% endmacro %}
