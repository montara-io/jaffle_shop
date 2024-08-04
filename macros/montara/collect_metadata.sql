{% macro get_schema_and_rowcount(relation) %}
  {% set column_query %}
  {% if target.type == 'snowflake' %}
  SELECT
    column_name,
    data_type
  FROM
    {{ relation.database }}.information_schema.columns
  WHERE
    table_schema = '{{ relation.schema }}'
    AND table_name = '{{ relation.identifier }}'
  ORDER BY
    ordinal_position {% elif target.type == 'redshift' %}
  SELECT
    column_name,
    data_type
  FROM
    information_schema.columns
  WHERE
    table_schema = '{{ relation.schema }}'
    AND table_name = '{{ relation.identifier }}'
  ORDER BY
    ordinal_position {% elif target.type == 'bigquery' %}
  SELECT
    column_name,
    data_type
  FROM
    `{{ relation.database }}.{{ relation.schema }}.INFORMATION_SCHEMA.COLUMNS`
  WHERE
    table_name = '{{ relation.identifier }}'
  ORDER BY
    ordinal_position {% elif target.type == 'postgres' %}
  SELECT
    column_name,
    data_type
  FROM
    information_schema.columns
  WHERE
    table_schema = '{{ relation.schema }}'
    AND table_name = '{{ relation.identifier }}'
  ORDER BY
    ordinal_position {% elif target.type == 'athena' %}
  SELECT
    column_name,
    data_type
  FROM
    information_schema.columns
  WHERE
    table_schema = '{{ relation.schema }}'
    AND table_name = '{{ relation.identifier }}'
  ORDER BY
    ordinal_position
  {% else %}
    {{ exceptions.raise_compiler_error(
      "Unsupported adapter type: " ~ target.type
    ) }}
  {% endif %}

  {% endset %}
  {% set columns = run_query(column_query) %}
  {% set column_info = [] %}
  {% for row in columns %}
    {% do column_info.append(
      { 'columnName': row [0],
      'dataType': row [1] }
    ) %}
  {% endfor %}

  {% set rowcount_query %}
SELECT
  COUNT(*) AS row_count
FROM
  {{ relation }}

  {% endset %}
  {% set row_count = run_query(rowcount_query).columns [0].values() [0] %}
  {% do return(
    { 'columns': column_info,
    'row_count': row_count }
  ) %}
{% endmacro %}

{% macro collect_metadata(model) %}
  {% if execute %}
    {# Collect model metadata #}
    {% set model_relation = adapter.get_relation(
      database = model.database,
      schema = model.schema,
      identifier = model.name
    ) %}
    {% if model_relation %}
      {% set schema_and_rowcount = get_schema_and_rowcount(model_relation) %}
      {% set model_metadata ={ model.unique_id:{ 'name': model.name,
      'unique_id': model.unique_id,
      'resource_type': 'model',
      'database': model.database,
      'schema': model.schema,
      'columns': schema_and_rowcount ['columns'],
      'row_count': schema_and_rowcount ['row_count'] }}
      %}
      {% set model_metadata_json = tojson(model_metadata) %}
      {% do log(
        "METADATA_JSON_START " ~ model_metadata_json ~ " METADATA_JSON_END",
        info = False
      ) %}
    {% endif %}

    {# Collect source metadata #}
    {% for node in graph.sources.values() %}
      {% if node.unique_id in model.depends_on.nodes %}
        {% set source_relation = adapter.get_relation(
          database = node.database,
          schema = node.schema,
          identifier = node.identifier
        ) %}
        {% if source_relation %}
          {% set schema_and_rowcount = get_schema_and_rowcount(source_relation) %}
          {% set source_metadata ={ node.unique_id:{ 'name': node.name,
          'unique_id': node.unique_id,
          'source_name': node.source_name,
          'resource_type': 'source',
          'database': node.database,
          'schema': node.schema,
          'columns': schema_and_rowcount ['columns'],
          'row_count': schema_and_rowcount ['row_count'] }}
          %}
          {% set source_metadata_json = tojson(source_metadata) %}
          {% do log(
            "METADATA_JSON_START " ~ source_metadata_json ~ " METADATA_JSON_END",
            info = False
          ) %}
        {% endif %}
      {% endif %}
    {% endfor %}
  {% endif %}
{% endmacro %}
