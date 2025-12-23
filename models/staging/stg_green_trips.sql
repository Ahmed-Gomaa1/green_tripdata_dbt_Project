-- models/staging/stg_green_trips.sql

with source as (
    select * from {{ source('raw', 'GREEN_TRIPDATA_2021_12') }}
),

stg_green_trips as (
    select
        -- Identifiers (standardize naming)
        vendorid as vendor_id,
        ratecodeid as rate_code_id,
        pulocationid as pickup_location_id,
        dolocationid as dropoff_location_id,
        payment_type as payment_type_id,
        trip_type as trip_type_id,
        
        -- Timestamps (convert from microseconds to proper timestamp)
        date_trunc('second', to_timestamp(lpep_pickup_datetime / 1000000)) as pickup_datetime,
        date_trunc('second', to_timestamp(lpep_dropoff_datetime / 1000000)) as dropoff_datetime,
        
        -- Trip information (pass through as-is)
        passenger_count,
        trip_distance,
        
        -- Financial amounts (pass through as-is)
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        congestion_surcharge,
        total_amount,
        
        -- Flags (pass through as-is)
        store_and_fwd_flag,
        ehail_fee            
    from source
)

select * from stg_green_trips