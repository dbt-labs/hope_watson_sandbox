{% macro parameterized_train_anomaly_model(
    model_name="basic_model",
    dataset_name="stg_historical_sales_data",  
    timestamp_colname="date",
    target_colname="sales"
) %}
    {% set query %}
        CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION {{ model_name }}(
            INPUT_DATA => TABLE({{ ref(dataset_name) }}),
            TIMESTAMP_COLNAME => '{{ timestamp_colname }}',
            TARGET_COLNAME => '{{ target_colname }}',
            LABEL_COLNAME => ''
        )
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}
