{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set env = env_var('DBT_CURRENT_ENV', '') -%}
    
    {% do log("Target Schema: " ~ target.schema, info=True) %}
    {% do log("Custom Schema Name: " ~ (custom_schema_name if custom_schema_name else 'None'), info=True) %}
    {% do log("Environment: " ~ env, info=True) %}

    {%- if env == 'dev' -%}
        {{ target.schema }}
    {%- elif env == 'prod' -%}
        {{ custom_schema_name | trim | default(target.schema) }}
    {%- else -%}
        {% do exceptions.raise_compiler_error("Invalid DBT_CURRENT_ENV. Must be 'dev' or 'prod'.") %}
    {%- endif -%}

{%- endmacro %}
