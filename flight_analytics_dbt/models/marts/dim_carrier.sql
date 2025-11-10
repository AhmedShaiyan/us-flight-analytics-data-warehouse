-- Derive carriers from flight data
WITH carriers AS (
    SELECT DISTINCT
        OP_UNIQUE_CARRIER as carrier_code
    FROM `flight-analytics-portfolio2001.raw.flights_raw`
    WHERE OP_UNIQUE_CARRIER IS NOT NULL
)

SELECT 
    carrier_code,
 
    CASE carrier_code
        WHEN 'AA' THEN 'American Airlines'
        WHEN 'DL' THEN 'Delta Air Lines'
        WHEN 'UA' THEN 'United Airlines'
        WHEN 'WN' THEN 'Southwest Airlines'
        WHEN 'B6' THEN 'JetBlue Airways'
        WHEN 'AS' THEN 'Alaska Airlines'
        WHEN 'NK' THEN 'Spirit Airlines'
        WHEN 'F9' THEN 'Frontier Airlines'
        WHEN 'G4' THEN 'Allegiant Air'
        WHEN 'SY' THEN 'Sun Country Airlines'
        WHEN 'HA' THEN 'Hawaiian Airlines'
        WHEN 'VX' THEN 'Virgin America'
        WHEN '9E' THEN 'Endeavor Air'
        WHEN 'YX' THEN 'Republic Airways'
        WHEN 'YV' THEN 'Mesa Airlines'
        WHEN 'OO' THEN 'SkyWest Airlines'
        WHEN 'OH' THEN 'PSA Airlines'
        WHEN 'MQ' THEN 'Envoy Air'
        WHEN 'EV' THEN 'ExpressJet'
        ELSE 'Others'  
    END as carrier_name
FROM carriers
ORDER BY carrier_code