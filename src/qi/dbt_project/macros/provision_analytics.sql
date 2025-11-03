{% macro provision_analytics() %}
  {% do run_query("CREATE DATABASE IF NOT EXISTS analytics") %}
  {{ log("Provisioned analytics DB ✅", info=True) }}
{% endmacro %}