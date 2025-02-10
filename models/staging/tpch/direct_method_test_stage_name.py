import snowflake.snowpark as snowpark

def model(dbt, session):
    # Retrieve schema dynamically using dbt.this.schema
    schema_name = dbt.this.schema  # Ensures correct schema is retrieved

    if not schema_name:
        raise ValueError("Schema could not be determined. Ensure dbt is properly configured.")

    # Construct a properly formatted stage name
    stage_name = f"{schema_name}_stage"

    # Correct Snowflake SQL for creating a stage
    session.sql(f"""
        CREATE OR REPLACE STAGE {schema_name}.{stage_name}
        FILE_FORMAT = (TYPE = PARQUET);
    """).collect()

    # Validate that the stage now exists
    result = session.sql(f"SHOW STAGES LIKE '{stage_name}' IN SCHEMA {schema_name};").collect()
    if not result:
        raise ValueError(f"Stage {schema_name}.{stage_name} was not created successfully.")

    # Example: Writing a DataFrame to the Snowflake stage
    df = session.create_dataframe([("value1", 1), ("value2", 2)], schema=["col1", "col2"])
    df.write.mode("overwrite").parquet(f"@{stage_name}/my_output.parquet")

    return df
