with 

source as (

    select * from {{ source('anomaly_detection_examples', 'historical_sales_data') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['store_id', 'item','date','sales']) }} as sk,
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
