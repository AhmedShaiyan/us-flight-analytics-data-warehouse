SELECT
    -- Keys
    flight_date,
    carrier_code,
    flight_number,
    origin_airport,
    dest_airport,
    
    -- Context
    origin_city,
    dest_city,
    
    -- Times
    scheduled_dep_time,
    actual_dep_time,
    scheduled_arr_time,
    actual_arr_time,
    
    -- Metrics
    dep_delay_minutes,
    arr_delay_minutes,
    air_time_minutes,
    actual_elapsed_time_minutes,
    
    -- Flags
    is_cancelled,
    is_diverted,
    cancellation_code,
    
    -- Delay attribution
    carrier_delay_minutes,
    weather_delay_minutes,
    nas_delay_minutes,
    security_delay_minutes,
    late_aircraft_delay_minutes,
    
    -- Calculated fields
    CASE 
        WHEN is_cancelled = 1 THEN 0
        WHEN arr_delay_minutes <= 15 THEN 1 
        ELSE 0 
    END as is_ontime

FROM {{ ref('stg_flights') }}