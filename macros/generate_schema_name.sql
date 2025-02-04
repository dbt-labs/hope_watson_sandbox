{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set env = env_var('DBT_CURRENT_ENV', '') -%}
    
    {# Ensure logging happens only once per run #}
    {% if not var('schema_log_shown', False) %}
        {% do log("DBT Run Environment: " ~ env, info=True) %}
        {% do log("DBT Target Schema: " ~ target.schema, info=True) %}
        {% do var('schema_log_shown', True) %}
    {% endif %}


    {%- if env == 'dev' -%}
        {{ target.schema }}
    {%- elif env == 'prod' -%}
        {# Ensure a valid schema is always assigned #}
        {{ (custom_schema_name | trim) if custom_schema_name else target.schema }}
    {%- else -%}
        {% do exceptions.raise_compiler_error("Invalid DBT_CURRENT_ENV. Must be 'dev' or 'prod'.") %}
    {%- endif -%}

{%- endmacro %}

