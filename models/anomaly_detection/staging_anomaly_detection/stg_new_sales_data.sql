with

source as (

    select * from {{ source('anomaly_detection_examples', 'new_sales_data') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['store_id', 'item','date','sales']) }} as sk,
        store_id,
        item,
        date,
        sales,
/* 
It is very important to create this placeholder label column.
This is so the DDL of the tables match between training and testing to prevent errors. 
*/
        null as label,
        temperature,
        humidity,
        holiday

    from source

)

select * from renamed
