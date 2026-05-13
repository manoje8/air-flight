{{
  config(
    materialized='table',
    tags=['gold', 'leaderboard'],
    partition_by={
      "field": "WINDOW_START",
      "data_type": "timestamp",
      "granularity": "hour"
    },
    cluster_by=['WINDOW_START', 'RANK']
  )
}}

-- Top-N busiest countries leaderboard by total flights per 30-min window
WITH windowed_flights AS (
  SELECT
    TIME_SLICE(TIME_POSITION::TIMESTAMP_NTZ, 30, 'MINUTE') AS WINDOW_START,
    ORIGIN_COUNTRY,
    COUNT(DISTINCT ICAO24) AS UNIQUE_FLIGHTS,
    COUNT(*) AS TOTAL_FLIGHTS
  FROM {{ ref('silver_flights_cleaned') }}
  WHERE TIME_POSITION IS NOT NULL
  GROUP BY 1, 2
),
ranked_countries AS (
  SELECT
    WINDOW_START,
    ORIGIN_COUNTRY,
    UNIQUE_FLIGHTS,
    TOTAL_FLIGHTS,
    RANK() OVER (PARTITION BY WINDOW_START ORDER BY TOTAL_FLIGHTS DESC) AS RANK,
    DENSE_RANK() OVER (PARTITION BY WINDOW_START ORDER BY TOTAL_FLIGHTS DESC) AS DENSE_RANK
  FROM windowed_flights
)

SELECT
  WINDOW_START,
  RANK,
  DENSE_RANK,
  ORIGIN_COUNTRY,
  UNIQUE_FLIGHTS,
  TOTAL_FLIGHTS,
  CURRENT_TIMESTAMP() AS LOAD_TIME
FROM ranked_countries
