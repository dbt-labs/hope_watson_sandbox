{% snapshot orders_snapshot_retrofit %}

{{
    config(
      unique_key='o_orderkey',
      schema='snapshots', 
      strategy='timestamp',
      updated_at='o_orderdate'
    )
}}

select * from {{ source('tpch', 'orders') }}

{% endsnapshot %}
