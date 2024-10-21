{% macro centralize_test_failures(results) %}
  {%- set test_results = [] -%}

  {%- for result in results -%}
    {%- if result.node.resource_type == 'test' and result.status != 'skipped' and (
          result.node.config.get('store_failures') or flags.STORE_FAILURES
      ) and result.status == 'fail'  -%}  {# Only consider tests that have failed #}
      
      {%- set unique_id = result.node.unique_id -%}
      
      {# Use the graph context to get depends_on.nodes #}
      {%- set depends_on_nodes = graph.nodes.get(unique_id).depends_on.nodes -%}

      {%- do test_results.append({
        'test_name': result.node.name,
        'unique_id': unique_id,
        'depends_on_nodes': depends_on_nodes
      }) -%}
    {%- endif -%}
  {%- endfor -%}

  {# Only proceed if there are failed tests #}
  {% if test_results | length > 0 %}
    {%- set central_tbl -%} {{ target.schema }}.test_failure_central {%- endset -%}
    {%- set history_tbl -%} {{ target.schema }}.test_failure_history {%- endset -%}

    {{ log("Test failures are being stored. Centralizing test failures in " + central_tbl, info = true) if execute }}

    create or replace table {{ central_tbl }} as (
      {% for result in test_results %}
        select
          '{{ result.test_name }}' as test_name,
          '{{ result.unique_id }}' as test_node,
          object_construct_keep_null(*) as test_failures_json,
          array_construct({% for node in result.depends_on_nodes %}'{{ node }}'{% if not loop.last %}, {% endif %}{% endfor %}) as model_names,
          current_timestamp as _timestamp
        {{ "union all" if not loop.last }}
      {% endfor %}
    );
  
    {# all to run in all environments including lower dev env #}
    {% if target.name == 'default' %}

      {{ log("Creating the history table " + history_tbl + " if it does not already exist.", info = true) if execute }}

      create table if not exists {{ history_tbl }} as (
        select 
          {{ dbt_utils.generate_surrogate_key(["test_name", "test_failures_json", "_timestamp"]) }} as sk_id, 
          * 
        from {{ central_tbl }}
        where false
      );

      {{ log("Inserting test failures from " + central_tbl + " into " + history_tbl, info = true) if execute }}

      insert into {{ history_tbl }} 
        select 
         {{ dbt_utils.generate_surrogate_key(["test_name", "test_failures_json", "_timestamp"]) }} as sk_id, 
         * 
        from {{ central_tbl }}
      ;
    {% endif %}

  {% else %}
    {{ log("No test failures to store. Ensure you passed the --store-failures if you want to store test failures. Example: dbt build --store-failures. If --store-failures was passed and no failures were stored it means there were no test failures!", info = true) if execute }}
  {% endif %}

{% endmacro %}