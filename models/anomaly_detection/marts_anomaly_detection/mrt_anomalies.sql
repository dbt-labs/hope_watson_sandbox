{{ config(materialized="table") }}

with anomaly_calc as (  
    select
        sk, 
        store_id,
        item,
        date,
        sales,
        z_score,  
        is_anomaly_calculation
    from {{ ref('int_statistician_anomaly_calculation') }}  
),

ml_anomaly_results as (
    select
        sk,
        store_id,
        item,
        date,
        sales,
        forecast,
        lower_bound,
        upper_bound,
        is_anomaly_algorithm
    from {{ ref('int_call_anomaly_detection') }}  
)

select 
    c.store_id,
    c.item,
    c.date,
    c.sales,
    c.z_score,
    ml.forecast,
    ml.lower_bound,
    ml.upper_bound,
    c.is_anomaly_calculation,
    ml.is_anomaly_algorithm,
    case 
        when c.is_anomaly_calculation = ml.is_anomaly_algorithm then true
        else false
    end as concordance
from anomaly_calc c
join ml_anomaly_results ml 
    on c.sk = ml.sk 
   
