import os
import snowflake.snowpark as snowpark

def model(dbt, session):
    # Set custom schema using dbt config
    dbt.config(
        schema="marketing_custom_schema"
    )

    # Retrieve schema using dbt.this.schema, reflecting custom schema naming
    schema_name = dbt.this.schema

    # Construct stage name
    stage_name = f"{schema_name}_stage"

    # Create stage if it does not exist
    session.sql(f"""
        CREATE STAGE IF NOT EXISTS {schema_name}.{stage_name}
        FILE_FORMAT = (TYPE = PARQUET);
    """).collect()

    # Write DataFrame to stage
    df = session.create_dataframe([("value1", 1), ("value2", 2)], schema=["col1", "col2"])
    df.write.mode("overwrite").parquet(f"@{schema_name}.{stage_name}/my_output.parquet", overwrite=True)

    return df
