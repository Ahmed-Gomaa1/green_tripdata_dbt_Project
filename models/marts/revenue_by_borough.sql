
with trips_with_locations as (
    select * from {{ ref('fct_green_trips') }}
),

borough_metrics as (
    select
        pickup_borough,
        
        -- Trip counts
        count(*) as total_trips,
        count(distinct pickup_zone_name) as unique_zones,
        
        -- Revenue
        sum(total_amount) as total_revenue,
        avg(total_amount) as avg_revenue_per_trip,
        sum(fare_amount) as total_fare,
        sum(tip_amount) as total_tips,
        
        -- Distance
        sum(trip_distance) as total_miles,
        avg(trip_distance) as avg_miles_per_trip,
        
        -- Efficiency
        avg(tip_percentage) as avg_tip_percentage,
        
        -- Trip characteristics
        avg(trip_duration_minutes) as avg_duration_minutes,
        avg(passenger_count) as avg_passengers
        
    from trips_with_locations
    where pickup_borough is not null
    group by 1
),

with_rankings as (
    select
        *,
        rank() over (order by total_revenue desc) as revenue_rank,
        rank() over (order by total_trips desc) as trip_count_rank,
        round(total_revenue / sum(total_revenue) over () * 100, 2) as pct_of_total_revenue
    from borough_metrics
)

select * from with_rankings
order by total_revenue desc