{{
  config(
    materialized='table',
    tags=['gold', 'heatmap'],
    partition_by={
      "field": "WINDOW_START",
      "data_type": "timestamp",
      "granularity": "hour"
    },
    cluster_by=['WINDOW_START', 'LATITUDE_BIN', 'LONGITUDE_BIN']
  )
}}

-- Flight density heatmap by latitude/longitude grid
SELECT
  DATE_TRUNC('HOUR', TIME_POSITION) AS WINDOW_START,
  ROUND(LATITUDE, 0) AS LATITUDE_BIN,
  ROUND(LONGITUDE, 0) AS LONGITUDE_BIN,

  -- Density metrics
  COUNT(DISTINCT ICAO24) AS UNIQUE_FLIGHTS,
  COUNT(*) AS TOTAL_POSITIONS,

  -- Flight attributes
  AVG(VELOCITY_VALIDATED) AS AVG_VELOCITY,
  AVG(BARO_ALTITUDE) AS AVG_BARO_ALTITUDE,
  SUM(CASE WHEN ON_GROUND = FALSE THEN 1 ELSE 0 END) AS AIRBORNE_POSITIONS,

  CURRENT_TIMESTAMP() AS LOAD_TIME

FROM {{ ref('silver_flights_cleaned') }}
WHERE TIME_POSITION IS NOT NULL
  AND LATITUDE IS NOT NULL
  AND LONGITUDE IS NOT NULL
  AND lat_valid = 1
  AND lon_valid = 1
GROUP BY 1, 2, 3
