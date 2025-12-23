{ % snapshot green_tripdata_2021_12_snapshot %}

{{
  config(
    target_schema='snapshots',
    unique_key='surrogate_key',
    strategy='check',
    check_cols='all'
  )
}}

with base as (
    select *,
           row_number() over (
               order by LPEP_PICKUP_DATETIME, LPEP_DROPOFF_DATETIME, VENDORID, PULOCATIONID
           ) as surrogate_key
    from {{ source('raw', 'GREEN_TRIPDATA_2021_12') }}
)

select *
from base

{% endsnapshot %}
