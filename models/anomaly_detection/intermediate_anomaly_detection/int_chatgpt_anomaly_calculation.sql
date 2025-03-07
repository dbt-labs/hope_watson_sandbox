with historical_distribution as (
    -- find the max date from historical sales data
    select max(date) as max_historical_date
    from {{ ref('stg_historical_sales_data') }}
),

historical_stats as (
    -- calculate mean and std deviation using historical data
    select 
        item,
        avg(sales) as mean_sales,
        stddev(sales) as std_sales
    from {{ ref('stg_historical_sales_data') }}, historical_distribution
    where date < max_historical_date 
    group by item
),

new_sales_analysis as (
    -- compare new sales data against historical mean/std
    select 
        n.store_id,
        n.item,
        n.date,
        n.sales,
        (n.sales - h.mean_sales) / nullif(h.std_sales, 0) as z_score
    from {{ ref('stg_new_sales_data') }} n
    join historical_stats h on n.item = h.item
)

select 
    store_id,
    item,
    date,
    sales,
    z_score,
    case when abs(z_score) > 3 then true else false end as is_anomaly
from new_sales_analysis
order by item, date