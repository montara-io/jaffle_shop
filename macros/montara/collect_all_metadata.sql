{% macro collect_all_metadata() %}
  {% set models = graph.nodes.values() | selectattr(
    "resource_type",
    "equalto",
    "model"
  ) | list %}
  {% for model in models %}
    {{ collect_metadata(model) }}
  {% endfor %}
{% endmacro %}
