with historical_distribution as (
    -- this is mostly as a placeholder if in the historical_stats cte you need to do date subtraction on your specific dataset
    select max(date) as max_historical_date
    from {{ ref('stg_historical_sales_data') }}
),

historical_stats as (
    select 
        item,
        avg(ln(sales + 1)) as log_mean_sales,  -- log-transform to prevent negative values
        stddev(ln(sales + 1)) as log_std_sales
    from {{ ref('stg_historical_sales_data') }}, historical_distribution
    where date < max_historical_date 
    group by item
),

new_sales_analysis as (
    -- compare new sales data against historical log-transformed mean/std
    select 
        n.sk, 
        n.store_id,
        n.item,
        n.date,
        n.sales,
        (ln(n.sales + 1) - h.log_mean_sales) / nullif(h.log_std_sales, 0) as z_score
    from {{ ref('stg_new_sales_data') }} n
    join historical_stats h on n.item = h.item
)

select 
    sk, 
    store_id,
    item,
    date,
    sales,
    z_score,
    case when abs(z_score) > 3 then true else false end as is_anomaly_calculation
from new_sales_analysis
order by item, date


