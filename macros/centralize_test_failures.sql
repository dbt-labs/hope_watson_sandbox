{% macro centralize_test_failures(results) %}
    {# --add "{{ centralize_test_failures(results) }}" to an on-run-end: block in dbt_project.yml #}
    {# --run with dbt build --store-failures. The next v.1.0.X release of dbt will include post run hooks for dbt test! #}
    {%- set test_results = [] -%}
    {%- for result in results -%}
    {%- if result.node.resource_type == 'test' and result.status != 'skipped' and (
          result.node.config.get('store_failures') or flags.STORE_FAILURES
      )
    -%}
            {%- do test_results.append(result) -%}
        {%- endif -%}
    {%- endfor -%}
  
    {%- set central_tbl -%} {{ target.schema }}.test_failure_central {%- endset -%}
    {%- set history_tbl -%} {{ target.schema }}.test_failure_history {%- endset -%}
  
    {{ log("Centralizing test failures in " + central_tbl, info = true) if execute }}

    create or replace table {{ central_tbl }} as (
  
    {% for result in test_results %}
        {% set model_name = None %}
        
        {# Try to use meta attribute as fallback for model name extraction #}
        {% if result.node.meta.get('model_name') is not none %}
          {% set model_name = result.node.meta.get('model_name') %}
        {% else %}
          {# Fallback to extracting model name from depends_on.nodes #}
          {%- for node in result.node.depends_on.nodes -%}
            {%- if node.startswith('model.') -%}
              {% set model_name = node.split('.')[1] %}
            {%- endif -%}
          {%- endfor -%}
        {% endif %}
  
      select
        '{{ result.node.name }}' as test_name,
        '{{ model_name }}' as model_name, -- Extracting model name from the parent model reference
        object_construct_keep_null(*) as test_failures_json,
        current_timestamp as _timestamp
        
      from {{ result.node.relation_name }}
      
      {{ "union all" if not loop.last }}
    
    {% endfor %}
    
  
  );
  
  -- all to run in all environments including lower dev env
  {% if target.name == 'default' %}
      create table if not exists {{ history_tbl }} as (
        select 
          {{ dbt_utils.generate_surrogate_key(["test_name", "test_failures_json", "_timestamp"]) }} as sk_id, 
          * 
        from {{ central_tbl }}
        where false
      );

    insert into {{ history_tbl }} 
      select 
       {{ dbt_utils.generate_surrogate_key(["test_name", "test_failures_json", "_timestamp"]) }} as sk_id, 
       * 
      from {{ central_tbl }}
    ;
  {% endif %}

{% endmacro %}
