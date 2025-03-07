{{ config(materialized="table") }}

with comparison as (
    select concordance
    from {{ ref('mrt_anomalies') }}  
)

select 
    count(*) as total_records,
    sum(case when concordance then 1 else 0 end) as matching_records,
    round((sum(case when concordance then 1 else 0 end) * 100.0 / count(*)), 2) as concordance_rate
from comparison
