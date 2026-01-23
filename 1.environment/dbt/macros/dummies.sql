{% macro _safe_colname(prefix, value) -%}
  {{ (prefix ~ '__' ~ value)
      | lower
      | replace(' ', '_')
      | replace('&', 'and')
      | replace('-', '_')
      | replace('/', '_')
      | replace('.', '')
      | replace(',', '')
      | replace('(', '')
      | replace(')', '')
      | replace("'", '')
      | replace('"', '')
  }}
{%- endmacro %}

{% macro generate_dummies(from_relation, col, prefix, drop_first=false) -%}
  {# from_relation: ref('...') o source('...','...') #}

  {% set q %}
    select distinct {{ col }} as v
    from {{ from_relation }}
    where {{ col }} is not null
    order by 1
  {% endset %}

  {% if execute %}
    {% set res = run_query(q) %}
    {% set values = res.columns[0].values() %}
  {% else %}
    {% set values = [] %}
  {% endif %}

  {% if drop_first and (values | length) > 0 %}
    {% set values = values[1:] %}
  {% endif %}

  {%- for v in values -%}
    , case when {{ col }} = '{{ v }}' then 1 else 0 end as {{ _safe_colname(prefix, v) }}
  {%- endfor -%}
{%- endmacro %}
