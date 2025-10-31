{#{% macro generate_database_name(custom_database_name, node) -%}#}
{% macro generate_database_name(custom_database_name, node) -%}
  {%- if custom_database_name is string and custom_database_name|length > 0 -%}
    {{ return(custom_database_name) }}
  {%- elif target is defined and target.database is defined -%}
    {{ return(target.database|string) }}
  {%- else -%}
    {{ return('default') }}
  {%- endif -%}
{%- endmacro %}

{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- set db = generate_database_name(custom_schema_name, node) -%}
  {{ return(db) }}
{%- endmacro %}