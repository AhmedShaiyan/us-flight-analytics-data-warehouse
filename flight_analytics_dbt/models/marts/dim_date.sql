WITH dates AS (
    SELECT DISTINCT 
        DATE(PARSE_TIMESTAMP('%m/%d/%Y %I:%M:%S %p', FL_DATE)) as date_day
    FROM `flight-analytics-portfolio2001.raw.flights_raw`
)

SELECT
    date_day,
    EXTRACT(YEAR FROM date_day) as year,
    EXTRACT(QUARTER FROM date_day) as quarter,
    EXTRACT(MONTH FROM date_day) as month,
    EXTRACT(DAY FROM date_day) as day_of_month,
    EXTRACT(DAYOFWEEK FROM date_day) as day_of_week,
    FORMAT_DATE('%A', date_day) as day_name,
    FORMAT_DATE('%B', date_day) as month_name,
    CASE WHEN EXTRACT(DAYOFWEEK FROM date_day) IN (1, 7) THEN 1 ELSE 0 END as is_weekend

FROM dates
ORDER BY date_day