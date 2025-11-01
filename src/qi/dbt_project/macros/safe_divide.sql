{% macro safe_div(num, den) -%}
  CASE
    WHEN {{ den }} = 0 OR {{ den }} IS NULL THEN NULL
    ELSE {{ num }} / {{ den }}
  END
{%- endmacro %}