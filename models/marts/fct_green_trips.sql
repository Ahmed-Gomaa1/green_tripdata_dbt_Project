
with enriched_trips as (
    select * from {{ ref('int_green_trips_enriched') }}),

pickup_zones as (
    select * from {{ source('raw', 'TAXI_ZONE_LOOKUP') }}),

dropoff_zones as (
    select * from {{ source('raw', 'TAXI_ZONE_LOOKUP') }}),

fct_green_trips as (
    select
        -- Trip identifiers
        {{ dbt_utils.generate_surrogate_key([
    'pickup_datetime',
    'vendor_id',
    'dropoff_datetime',
    'pickup_location_id',
    'dropoff_location_id'
       ]) }} as trip_key,
        
        -- Timestamps
        t.pickup_datetime,
        t.dropoff_datetime,
        t.pickup_date,
        t.pickup_day_of_week,
        t.pickup_day_name,
        
        -- Vendor
        t.vendor_id,
        case 
            when t.vendor_id = 1 then 'Creative Mobile Technologies'
            when t.vendor_id = 2 then 'Curb Mobility'
            when t.vendor_id = 0 then 'Myle Technologies'
            else 'Unknown'
        end as vendor_name,
        
        -- Pickup location
        t.pickup_location_id,
        pu.borough as pickup_borough,
        pu.zone as pickup_zone_name,
        pu.service_zone as pickup_service_zone,
        
        -- Dropoff location
        t.dropoff_location_id,
        do.borough as dropoff_borough,
        do.zone as dropoff_zone_name,
        do.service_zone as dropoff_service_zone,
        
        -- Trip metrics
        t.passenger_count,
        t.trip_distance,
        t.trip_duration_minutes,
        t.distance_category,
        
        -- Time categories
        t.time_of_day_category,
        t.day_type,
        
        -- Financial
        t.fare_amount,
        t.extra,
        t.mta_tax,
        t.tip_amount,
        t.tolls_amount,
        t.improvement_surcharge,
        t.congestion_surcharge,
        t.total_amount,
        t.tip_percentage,
        t.base_charges,
        
        -- Payment info
        t.payment_type_id,
      
        
        -- Rate and trip type
        t.rate_code_id,
        t.trip_type_id,
        
        -- Flags
        t.store_and_fwd_flag,
        t.ehail_fee
        
    from enriched_trips t
    left join pickup_zones pu 
        on t.pickup_location_id = pu.locationid
    left join dropoff_zones do 
        on t.dropoff_location_id = do.locationid
)

select * from fct_green_trips