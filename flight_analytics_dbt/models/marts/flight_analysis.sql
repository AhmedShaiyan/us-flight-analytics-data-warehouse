{{
    config(
        materialized='view'
    )
}}

SELECT 
    f.flight_date,
    f.flight_number,
    
    -- Carrier info
    c.carrier_name,
    f.carrier_code,
    
    -- Origin details
    f.origin_airport as origin_code,
    orig.airport_name as origin_airport_name,
    orig.city as origin_city,
    orig.state as origin_state,
    orig.latitude as origin_lat,
    orig.longitude as origin_lon,
    
    -- Destination details
    f.dest_airport as dest_code,
    dest.airport_name as dest_airport_name,
    dest.city as dest_city,
    dest.state as dest_state,
    dest.latitude as dest_lat,
    dest.longitude as dest_lon,
    
    -- Date dimensions
    d.day_name,
    d.month_name,
    d.is_weekend,
    d.quarter,
    
    -- Formatted times (convert 730 to "07:30")
    CASE 
        WHEN f.scheduled_dep_time IS NOT NULL THEN
            CONCAT(
                LPAD(CAST(CAST(f.scheduled_dep_time / 100 AS INT64) AS STRING), 2, '0'),
                ':',
                LPAD(CAST(MOD(f.scheduled_dep_time, 100) AS STRING), 2, '0')
            )
        ELSE NULL
    END as scheduled_dep_time_formatted,
    
    CASE 
        WHEN f.actual_dep_time IS NOT NULL THEN
            CONCAT(
                LPAD(CAST(CAST(f.actual_dep_time / 100 AS INT64) AS STRING), 2, '0'),
                ':',
                LPAD(CAST(MOD(CAST(f.actual_dep_time AS INT64), 100) AS STRING), 2, '0')
            )
        ELSE NULL
    END as actual_dep_time_formatted,
    
    CASE 
        WHEN f.scheduled_arr_time IS NOT NULL THEN
            CONCAT(
                LPAD(CAST(CAST(f.scheduled_arr_time / 100 AS INT64) AS STRING), 2, '0'),
                ':',
                LPAD(CAST(MOD(f.scheduled_arr_time, 100) AS STRING), 2, '0')
            )
        ELSE NULL
    END as scheduled_arr_time_formatted,
    
    CASE 
        WHEN f.actual_arr_time IS NOT NULL THEN
            CONCAT(
                LPAD(CAST(CAST(f.actual_arr_time / 100 AS INT64) AS STRING), 2, '0'),
                ':',
                LPAD(CAST(MOD(CAST(f.actual_arr_time AS INT64), 100) AS STRING), 2, '0')
            )
        ELSE NULL
    END as actual_arr_time_formatted,
    
    -- Keep original times as integers for calculations
    f.scheduled_dep_time,
    f.actual_dep_time,
    f.scheduled_arr_time,
    f.actual_arr_time,
    
    -- Flight metrics
    f.dep_delay_minutes,
    f.arr_delay_minutes,
    f.air_time_minutes,
    f.actual_elapsed_time_minutes,
    
    -- Delay breakdown
    f.carrier_delay_minutes,
    f.weather_delay_minutes,
    f.nas_delay_minutes,
    f.security_delay_minutes,
    f.late_aircraft_delay_minutes,
    
    -- Status flags
    f.is_cancelled,
    f.is_diverted,
    f.cancellation_code,
    f.is_ontime,
    
    -- Delay category
    CASE
        WHEN f.is_cancelled = 1 THEN 'Cancelled'
        WHEN f.is_diverted = 1 THEN 'Diverted'
        WHEN f.arr_delay_minutes IS NULL THEN 'Unknown'
        WHEN f.arr_delay_minutes <= 0 THEN 'Early'
        WHEN f.arr_delay_minutes <= 15 THEN 'On-Time (0-15 min)'
        WHEN f.arr_delay_minutes <= 60 THEN 'Delayed (15-60 min)'
        ELSE 'Severely Delayed (60+ min)'
    END as delay_category,
    
    -- Route identifiers
    CONCAT(orig.city, ' → ', dest.city) as route_name,
    CONCAT(f.origin_airport, '-', f.dest_airport) as route_code

FROM {{ ref('fact_flights') }} f
LEFT JOIN {{ ref('dim_carrier') }} c 
    ON f.carrier_code = c.carrier_code
LEFT JOIN {{ ref('dim_airport') }} orig
    ON f.origin_airport = orig.airport_code
LEFT JOIN {{ ref('dim_airport') }} dest
    ON f.dest_airport = dest.airport_code
LEFT JOIN {{ ref('dim_date') }} d
    ON f.flight_date = d.date_day