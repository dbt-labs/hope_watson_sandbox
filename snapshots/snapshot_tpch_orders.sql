{% snapshot orders_snapshot %}

{{
    config(
      unique_key = ['o_orderkey', 'o_orderdate'],
      schema='snapshots', 
      strategy='check',
      check_cols=[
            'o_orderkey',
            'o_custkey',
            'o_orderstatus',
            'o_totalprice',
            'o_orderdate',
            'o_orderpriority',
            'o_clerk',
            'o_shippriority',
            'o_comment'
        ],
      snapshot_meta_column_names={
              'dbt_valid_from': 'valid_from',
              'dbt_valid_to': 'valid_to'
      }
    )
}}

select * from {{ source('tpch', 'orders') }}

{% endsnapshot %}

