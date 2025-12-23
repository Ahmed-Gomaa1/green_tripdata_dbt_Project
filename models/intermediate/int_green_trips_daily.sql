
with enriched_trips as (
    select * from {{ ref('int_green_trips_enriched') }}
),

int_green_trips_daily as (
    select
        pickup_date,
        pickup_day_name,
        day_type,
        
        -- Trip counts
        count(*) as total_trips,
        count(distinct vendor_id) as active_vendors,
        count(distinct pickup_location_id) as unique_pickup_locations,
        count(distinct dropoff_location_id) as unique_dropoff_locations,
        
        -- Distance metrics
        sum(trip_distance) as total_distance_miles,
        avg(trip_distance) as avg_distance_miles,
        min(trip_distance) as min_distance_miles,
        max(trip_distance) as max_distance_miles,
        
        -- Duration metrics
        avg(trip_duration_minutes) as avg_duration_minutes,
        min(trip_duration_minutes) as min_duration_minutes,
        max(trip_duration_minutes) as max_duration_minutes,
        
        -- Speed metrics
        avg(avg_speed_mph) as avg_speed_mph,
        
        -- Passenger metrics
        sum(passenger_count) as total_passengers,
        avg(passenger_count) as avg_passengers_per_trip,
        
        -- Revenue metrics
        sum(fare_amount) as total_fare_amount,
        sum(tip_amount) as total_tips,
        sum(tolls_amount) as total_tolls,
        sum(extra) as total_extras,
        sum(total_amount) as total_revenue,
        avg(total_amount) as avg_revenue_per_trip,
        avg(revenue_per_mile) as avg_revenue_per_mile,
        avg(revenue_per_passenger) as avg_revenue_per_passenger,
        
        -- Tip analysis
        avg(tip_percentage) as avg_tip_percentage,
        sum(case when is_high_tip then 1 else 0 end) as high_tip_count,
        
        -- Payment breakdown
        count(case when payment_type_id = 1 then 1 end) as credit_card_trips,
        count(case when payment_type_id = 2 then 1 end) as cash_trips,
        sum(case when payment_type_id = 1 then total_amount else 0 end) as credit_card_revenue,
        sum(case when payment_type_id = 2 then total_amount else 0 end) as cash_revenue,
        
        -- Trip type breakdown
        sum(case when is_long_trip then 1 else 0 end) as long_trips,
        sum(case when is_airport_trip then 1 else 0 end) as airport_trips,
        sum(case when is_shared_ride then 1 else 0 end) as shared_rides,
        
        -- Distance category distribution
        sum(case when distance_category = 'Very Short (< 1 mi)' then 1 else 0 end) as very_short_trips,
        sum(case when distance_category = 'Short (1-3 mi)' then 1 else 0 end) as short_trips,
        sum(case when distance_category = 'Medium (3-10 mi)' then 1 else 0 end) as medium_trips,
        sum(case when distance_category = 'Long (10+ mi)' then 1 else 0 end) as long_distance_trips
        
    from enriched_trips
    group by 1, 2, 3
)

select * from int_green_trips_daily
order by pickup_date