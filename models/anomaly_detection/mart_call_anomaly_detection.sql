-- create mart_call_anomaly_detection.sql model 
/* 
We must configure this model to materialize as a table.
This is because we use the snowflake TABLE() command in our query. 
*/
{{config(materialized="table")}}

SELECT *
FROM TABLE(basic_model!DETECT_ANOMALIES(
  INPUT_DATA => TABLE({{ ref('stg_new_sales_data') }}),
  TIMESTAMP_COLNAME => 'date',
  TARGET_COLNAME => 'sales'
))