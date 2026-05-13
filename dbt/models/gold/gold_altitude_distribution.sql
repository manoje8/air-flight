{{
  config(
    materialized='table',
    tags=['gold', 'altitude_distribution'],
    partition_by={
      "field": "WINDOW_START",
      "data_type": "timestamp",
      "granularity": "hour"
    },
    cluster_by=['WINDOW_START', 'ORIGIN_COUNTRY', 'ALTITUDE_BIN']
  )
}}

-- Altitude distribution histogram per origin country
WITH binned_flights AS (
  SELECT
    DATE_TRUNC('HOUR', TIME_POSITION) AS WINDOW_START,
    ORIGIN_COUNTRY,
    -- Bin barometric altitude into 1000 meter intervals
    FLOOR(BARO_ALTITUDE / 1000) * 1000 AS ALTITUDE_BIN,
    ICAO24
  FROM {{ ref('silver_flights_cleaned') }}
  WHERE TIME_POSITION IS NOT NULL
    AND BARO_ALTITUDE IS NOT NULL
)

SELECT
  WINDOW_START,
  ORIGIN_COUNTRY,
  ALTITUDE_BIN,
  COUNT(*) AS TOTAL_POSITIONS,
  COUNT(DISTINCT ICAO24) AS UNIQUE_FLIGHTS,
  CURRENT_TIMESTAMP() AS LOAD_TIME
FROM binned_flights
GROUP BY 1, 2, 3
