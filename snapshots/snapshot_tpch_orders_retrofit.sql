{% snapshot orders_snapshot_retrofit %}

{{
    config(
      unique_key='o_orderkey',
      schema='snapshots', 
      strategy='timestamp',
      updated_at='o_orderdate',
       snapshot_meta_column_names={
              'dbt_valid_from': 'valid_from',
              'dbt_valid_to': 'valid_to' },
      enabled=false
    )
}}

select * from {{ source('tpch', 'orders') }}

{% endsnapshot %}
