{% macro train_anomaly_model(model_name) %}
    {% set query %}
        CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION {{ model_name }}(
            INPUT_DATA => TABLE(historical_sales_data),
            TIMESTAMP_COLNAME => 'date',
            TARGET_COLNAME => 'sales',
            LABEL_COLNAME => ''
        );
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}
