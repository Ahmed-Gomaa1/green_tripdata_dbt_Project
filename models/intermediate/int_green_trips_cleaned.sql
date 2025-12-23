
with stg_trips as (
    select * from {{ ref('stg_green_trips') }}
),

int_green_trips_cleaned as (
    select *
    from stg_trips
    where 
        pickup_datetime < dropoff_datetime
        and trip_distance > 0
        and trip_distance < 500  
        and datediff(minute, pickup_datetime, dropoff_datetime) > 0
        and datediff(minute, pickup_datetime, dropoff_datetime) < 1440  -- Less than 24 hours

)

select * from int_green_trips_cleaned