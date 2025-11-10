WITH source AS (
    SELECT * FROM `flight-analytics-portfolio2001.raw.flights_raw`
)

SELECT
    -- Date and IDs
    PARSE_TIMESTAMP('%m/%d/%Y %I:%M:%S %p', FL_DATE) as flight_timestamp,
    DATE(PARSE_TIMESTAMP('%m/%d/%Y %I:%M:%S %p', FL_DATE)) as flight_date,
    OP_UNIQUE_CARRIER as carrier_code,
    OP_CARRIER_FL_NUM as flight_number,
    ORIGIN as origin_airport,
    ORIGIN_CITY_NAME as origin_city,
    DEST as dest_airport,
    DEST_CITY_NAME as dest_city,
    
    -- Times (stored as integers like 1430 for 2:30 PM)
    CRS_DEP_TIME as scheduled_dep_time,
    DEP_TIME as actual_dep_time,
    CRS_ARR_TIME as scheduled_arr_time,
    ARR_TIME as actual_arr_time,
    
    -- Delay metrics
    COALESCE(DEP_DELAY, 0) as dep_delay_minutes,
    COALESCE(ARR_DELAY, 0) as arr_delay_minutes,
    
    -- Status flags
    CAST(COALESCE(CANCELLED, 0) AS INT64) as is_cancelled,
    CAST(COALESCE(DIVERTED, 0) AS INT64) as is_diverted,
    CANCELLATION_CODE as cancellation_code,
    
    -- Flight metrics
    ACTUAL_ELAPSED_TIME as actual_elapsed_time_minutes,
    AIR_TIME as air_time_minutes,
    
    -- Delay breakdown
    COALESCE(CARRIER_DELAY, 0) as carrier_delay_minutes,
    COALESCE(WEATHER_DELAY, 0) as weather_delay_minutes,
    COALESCE(NAS_DELAY, 0) as nas_delay_minutes,
    COALESCE(SECURITY_DELAY, 0) as security_delay_minutes,
    COALESCE(LATE_AIRCRAFT_DELAY, 0) as late_aircraft_delay_minutes

FROM source
WHERE OP_UNIQUE_CARRIER IS NOT NULL
  AND ORIGIN IS NOT NULL
  AND DEST IS NOT NULL