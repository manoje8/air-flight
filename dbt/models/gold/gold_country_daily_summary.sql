{{
  config(
    materialized='table',
    tags=['gold', 'daily_summary'],
    partition_by={
      "field": "DATE",
      "data_type": "date",
      "granularity": "day"
    }
  )
}}

-- Daily summary per country
WITH weekly_comparison AS (
  SELECT
    DATE_TRUNC('DAY', WINDOW_START) AS DATE,
    ORIGIN_COUNTRY,
    SUM(TOTAL_FLIGHTS) AS DAILY_FLIGHTS,
    AVG(AVG_VELOCITY) AS DAILY_AVG_VELOCITY,
    SUM(AIRBORNE_COUNT) AS DAILY_AIRBORNE,
    SUM(ON_GROUND_COUNT) AS DAILY_ON_GROUND,
    SUM(ON_GROUND_COUNT) / NULLIF(SUM(TOTAL_FLIGHTS), 0) AS ON_GROUND_RATIO,

    -- 7-day moving average
    AVG(SUM(TOTAL_FLIGHTS)) OVER (
      PARTITION BY ORIGIN_COUNTRY
      ORDER BY DATE_TRUNC('DAY', WINDOW_START)
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS FLIGHTS_7DAY_AVG

  FROM {{ ref('gold_flight_aggregations') }}
  GROUP BY 1, 2
)

SELECT
  DATE,
  ORIGIN_COUNTRY,
  DAILY_FLIGHTS,
  DAILY_AVG_VELOCITY,
  DAILY_AIRBORNE,
  DAILY_ON_GROUND,
  ON_GROUND_RATIO,
  FLIGHTS_7DAY_AVG,

  -- Day-over-day change
  (DAILY_FLIGHTS - LAG(DAILY_FLIGHTS) OVER (
    PARTITION BY ORIGIN_COUNTRY ORDER BY DATE
  )) AS DOD_CHANGE,

  CURRENT_TIMESTAMP() AS LOAD_TIME

FROM weekly_comparison
