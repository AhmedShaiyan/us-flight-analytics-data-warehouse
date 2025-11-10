WITH kaggle_airports AS (
    SELECT 
        IATA as airport_code,
        AIRPORT as airport_name,
        CITY as city,
        STATE as state,
        COUNTRY as country,
        LATITUDE as latitude,
        LONGITUDE as longitude
    FROM `flight-analytics-portfolio2001.raw.airports_raw`
    WHERE IATA IS NOT NULL
      AND COUNTRY = 'USA'
),

-- Add missing airports from flight data
missing_from_origin AS (
    SELECT DISTINCT
        ORIGIN as airport_code,
        ORIGIN as airport_name,
        SPLIT(ORIGIN_CITY_NAME, ', ')[SAFE_OFFSET(0)] as city,
        SPLIT(ORIGIN_CITY_NAME, ', ')[SAFE_OFFSET(1)] as state,
        'USA' as country,
        CAST(NULL AS FLOAT64) as latitude,
        CAST(NULL AS FLOAT64) as longitude
    FROM `flight-analytics-portfolio2001.raw.flights_raw`
    WHERE ORIGIN NOT IN (SELECT airport_code FROM kaggle_airports)
),

missing_from_dest AS (
    SELECT DISTINCT
        DEST as airport_code,
        DEST as airport_name,
        SPLIT(DEST_CITY_NAME, ', ')[SAFE_OFFSET(0)] as city,
        SPLIT(DEST_CITY_NAME, ', ')[SAFE_OFFSET(1)] as state,
        'USA' as country,
        CAST(NULL AS FLOAT64) as latitude,
        CAST(NULL AS FLOAT64) as longitude
    FROM `flight-analytics-portfolio2001.raw.flights_raw`
    WHERE DEST NOT IN (SELECT airport_code FROM kaggle_airports)
),

all_missing AS (
    SELECT * FROM missing_from_origin
    UNION DISTINCT
    SELECT * FROM missing_from_dest
)


SELECT * FROM kaggle_airports
UNION ALL
SELECT * FROM all_missing