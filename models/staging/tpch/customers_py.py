import snowflake.snowpark.functions as F

def model(dbt, session):
    dbt.config(
        enabled=False
    )
    my_sql_model_df = dbt.ref("stg_tpch_customers")
    
    final_df = my_sql_model_df  # Modify as needed
    
    stage_name = "@my_snowflake_stage"
    file_path = "customers_data.parquet"
    
    final_df.write.mode("overwrite").format("parquet").save(f"{stage_name}/{file_path}")
    
    return final_df
