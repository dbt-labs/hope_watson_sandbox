-- create mart_call_anomaly_detection.sql model 
/* 
We must configure this model to materialize as a table.
This is because we use the snowflake TABLE() command in our query. 
*/
{{config(materialized="table")}}

with anomaly_results as (
    select 
      *
    from table(basic_model!DETECT_ANOMALIES(
        INPUT_DATA => table({{ ref('stg_new_sales_data') }}),
        TIMESTAMP_COLNAME => 'date',
        TARGET_COLNAME => 'sales',
        CONFIG_OBJECT => object_construct('prediction_interval', 0.997)  -- Match Z-score > 3
    ))
)

select 
    n.sk,  -- Preserve surrogate key from original data
    n.store_id,
    n.item,
    n.date,
    n.sales,
    a.forecast,
    a.lower_bound,
    a.upper_bound,
    a.is_anomaly as is_anomaly_algorithm
from {{ ref('stg_new_sales_data') }} n
join anomaly_results a 
    on n.date = a.ts  -- Match timestamp
    and n.sales = a.y  -- Match original sales values
order by n.item, n.date



