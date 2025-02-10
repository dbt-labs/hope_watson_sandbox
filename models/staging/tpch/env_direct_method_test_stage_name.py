import os
import snowflake.snowpark as snowpark

def model(dbt, session):
    # Get environment and base schema
    env = os.getenv('DBT_MY_ENV', 'dev')
    base_schema = dbt.config.get('schema') or dbt.this.schema

    # Determine schema based on environment
    if env in ['prod', 'qa']:
        schema_name = f"{base_schema}"
    elif env == 'dev':
        schema_name = f"{base_schema}"
    else:
        schema_name = f"{base_schema}_staging"

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
