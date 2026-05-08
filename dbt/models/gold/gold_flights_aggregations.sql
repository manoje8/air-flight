{{
  config(
    materialized='table',
    tags=['gold', 'aggregation'],
    partition_by={
      "field": "WINDOW_START",
      "data_type": "timestamp",
      "granularity": "hour"
    },
    cluster_by=['WINDOW_START', 'ORIGIN_COUNTRY']
  )
}}

-- Hourly aggregations by country
SELECT
  DATE_TRUNC('HOUR', TIME_POSITION) AS WINDOW_START,
  ORIGIN_COUNTRY,

  -- Flight metrics
  COUNT(DISTINCT ICAO24) AS UNIQUE_FLIGHTS,
  COUNT(*) AS TOTAL_FLIGHTS,

  -- Velocity metrics
  AVG(VELOCITY_VALIDATED) AS AVG_VELOCITY,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY VELOCITY_VALIDATED) AS MEDIAN_VELOCITY,
  MAX(VELOCITY_VALIDATED) AS MAX_VELOCITY,

  -- Position metrics
  SUM(CASE WHEN ON_GROUND = TRUE THEN 1 ELSE 0 END) AS ON_GROUND_COUNT,
  SUM(CASE WHEN ON_GROUND = FALSE THEN 1 ELSE 0 END) AS AIRBORNE_COUNT,

  -- Altitude metrics
  AVG(BARO_ALTITUDE) AS AVG_BARO_ALTITUDE,
  AVG(GEO_ALTITUDE) AS AVG_GEO_ALTITUDE,

  -- Data quality metrics
  AVG(lat_valid * lon_valid) * 100 AS POSITION_VALIDITY_PCT,
  SUM(CASE WHEN airborne_position_valid = 1 THEN 1 ELSE 0 END) AS VALID_AIRBORNE_POSITIONS,

  CURRENT_TIMESTAMP() AS LOAD_TIME

FROM {{ ref('silver_flights_cleaned') }}
WHERE TIME_POSITION IS NOT NULL
GROUP BY 1, 2