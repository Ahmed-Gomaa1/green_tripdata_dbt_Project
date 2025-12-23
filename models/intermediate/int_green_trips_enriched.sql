
with cleaned_trips as (
    select * from {{ ref('int_green_trips_cleaned') }}
),

int_green_trips_enriched as (
    select
        -- Pass through all original fields
        *,
        
        -- === TIME-BASED CALCULATIONS ===
        datediff(minute, pickup_datetime, dropoff_datetime) as trip_duration_minutes,
        date(pickup_datetime) as pickup_date,
        dayofweek(pickup_datetime) as pickup_day_of_week,
        dayname(pickup_datetime) as pickup_day_name,
        
        -- Time of day categorization
        case 
            when hour(pickup_datetime) between 6 and 9 then 'Morning Rush'
            when hour(pickup_datetime) between 16 and 19 then 'Evening Rush'
            when hour(pickup_datetime) between 22 and 23 
                 or hour(pickup_datetime) between 0 and 5 then 'Late Night'
            else 'Off Peak'
        end as time_of_day_category,
        
        -- Weekend vs Weekday
        case 
            when dayofweek(pickup_datetime) in (5, 6) then 'Weekend'
            else 'Weekday'
        end as day_type,
        
        -- === DISTANCE CATEGORIZATION ===
        case 
            when trip_distance < 1 then 'Very Short (< 1 mi)'
            when trip_distance < 3 then 'Short (1-3 mi)'
            when trip_distance < 10 then 'Medium (3-10 mi)'
            else 'Long (10+ mi)'
        end as distance_category,
        
        -- === FINANCIAL CALCULATIONS ===
        -- Tip percentage (only meaningful for credit card payments)
        round(tip_amount / nullif(fare_amount, 0) * 100, 2) as tip_percentage,
        
        -- Revenue per mile
        round(total_amount / nullif(trip_distance, 0), 2) as revenue_per_mile,
        
        -- Base charges (everything except tips and tolls)
        fare_amount + extra + mta_tax + improvement_surcharge + congestion_surcharge as base_charges,
        
        -- === SPEED CALCULATION ===
        -- Average speed in MPH
        round(
            trip_distance / nullif(datediff(minute, pickup_datetime, dropoff_datetime) / 60.0, 0), 
            2
        ) as avg_speed_mph,
        
        -- === PASSENGER EFFICIENCY ===
        -- Revenue per passenger
        round(total_amount / nullif(passenger_count, 0), 2) as revenue_per_passenger,
        
        -- === QUALITY FLAGS ===
        -- High tip indicator (> 20%)
        case 
            when tip_amount / nullif(fare_amount, 0) > 0.20 then true
            else false
        end as is_high_tip,
        
        -- Long trip indicator (> 30 minutes)
        case 
            when datediff(minute, pickup_datetime, dropoff_datetime) > 30 then true
            else false
        end as is_long_trip,
        
        -- Airport trip indicator (common airport location IDs)
        case 
            when pickup_location_id in (132, 138) or dropoff_location_id in (132, 138) then true
            else false
        end as is_airport_trip,
        
        -- Shared ride indicator
        case 
            when passenger_count > 1 then true
            else false
        end as is_shared_ride
        
    from cleaned_trips
)

select * from int_green_trips_enriched