from datetime import datetime, timedelta
from snowflake.snowpark.functions import lit, col

def model(dbt, session):
    run_frequency = dbt.config.get('run_frequency','Daily')

    # Reference the source table
    final_df = dbt.ref("fct_tpch_orders")

    # Apply logic based on the run frequency and add execution_frequency column
    if run_frequency == 'Daily':
        # Daily run: select everything and add execution_frequency column
        final_df = final_df.with_column("execution_frequency", lit("Daily"))

    elif run_frequency == 'Weekly':
        # Weekly run: filter by a specific date range, e.g., last week, and add execution_frequency column
        final_df = final_df.with_column("execution_frequency", lit("Weekly"))

    else:
        raise ValueError(f"Unknown run_frequency: {run_frequency}. Expected 'Daily' or 'Weekly'.")

    # Return the final DataFrame with the execution_frequency column
    return final_df


