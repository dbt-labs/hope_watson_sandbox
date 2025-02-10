{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- set env = env_var('DBT_MY_ENV', '') -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- elif env == 'dev' -%}
        {{ default_schema }}
    {%- elif env in ['prod', 'qa'] -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}
