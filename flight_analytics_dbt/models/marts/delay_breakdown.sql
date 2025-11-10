SELECT 
    carrier_name,
    'Carrier Delay' as delay_type,
    SUM(carrier_delay_minutes) as delay_minutes
FROM {{ ref('flight_analysis') }}
GROUP BY carrier_name

UNION ALL

SELECT 
    carrier_name,
    'Weather Delay' as delay_type,
    SUM(weather_delay_minutes) as delay_minutes
FROM {{ ref('flight_analysis') }}
GROUP BY carrier_name

UNION ALL

SELECT 
    carrier_name,
    'NAS Delay' as delay_type,
    SUM(nas_delay_minutes) as delay_minutes
FROM {{ ref('flight_analysis') }}
GROUP BY carrier_name

UNION ALL

SELECT 
    carrier_name,
    'Security Delay' as delay_type,
    SUM(security_delay_minutes) as delay_minutes
FROM {{ ref('flight_analysis') }}
GROUP BY carrier_name

UNION ALL

SELECT 
    carrier_name,
    'Late Aircraft Delay' as delay_type,
    SUM(late_aircraft_delay_minutes) as delay_minutes
FROM {{ ref('flight_analysis') }}
GROUP BY carrier_name