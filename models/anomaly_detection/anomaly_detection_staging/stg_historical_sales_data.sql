with 

source as (

    select * from {{ source('anomaly_detection_examples', 'historical_sales_data') }}

),

renamed as (

    select
        store_id,
        item,
        date,
        sales,
        label,
        temperature,
        humidity,
        holiday

    from source

)

select * from renamed
