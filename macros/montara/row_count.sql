{% macro row_count() %}
    {% if execute %}
        {% set row_count_query %}
    SELECT
        COUNT(*) AS row_count
    FROM
        {{ this }}

        {% endset %}
        {% set results = run_query(row_count_query) %}
        {% set row_count = results.columns [0].values() [0] %}
        {% set model_name = model.name %}
        {{ log(
            "ROW_COUNT_LOG|model:" ~ model_name ~ "|count:" ~ row_count,
            info = True
        ) }}
    {% endif %}
{% endmacro %}
