{{
  config(
    materialized='table',
    tags=['silver', 'cleaned'],
    cluster_by=['ingestion_time', 'origin_country']
  )
}}

-- Remove duplicates, handle nulls, add data quality flags
WITH deduped AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY icao24, time_position
      ORDER BY ingestion_time DESC
    ) AS rn
  FROM {{ ref('bronze_flights_raw') }}
)

SELECT
  ICAO24,
  COALESCE(ORIGIN_COUNTRY, 'UNKNOWN') AS ORIGIN_COUNTRY,
  LATITUDE,
  LONGITUDE,
  TIME_POSITION,
  LAST_CONTACT,
  VELOCITY,
  CASE
    WHEN VELOCITY IS NOT NULL AND VELOCITY >= 0 THEN VELOCITY
    ELSE NULL
  END AS VELOCITY_VALIDATED,
  VERTICAL_RATE,
  TRUE_TRACK,
  BARO_ALTITUDE,
  GEO_ALTITUDE,
  ON_GROUND,
  INGESTION_TIME,

  -- Data quality flags
  CASE
    WHEN LATITUDE BETWEEN -90 AND 90 THEN 1
    ELSE 0
  END AS lat_valid,
  CASE
    WHEN LONGITUDE BETWEEN -180 AND 180 THEN 1
    ELSE 0
  END AS lon_valid,
  CASE
    WHEN ON_GROUND = FALSE AND LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
    THEN 1 ELSE 0
  END AS airborne_position_valid

FROM deduped
WHERE rn = 1  -- Keeping latest record per flight per timestamp