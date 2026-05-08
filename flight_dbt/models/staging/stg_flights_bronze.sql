-- Staging model to clean up raw Bronze data
WITH raw_bronze AS (
    SELECT *
    FROM {{ source('flight_schema', 'FLIGHT_BRONZE') }}
)
SELECT
    WINDOW_START,
    ICAO24,
    CALLSIGN,
    ORIGIN_COUNTRY,
    TIME_POSITION,
    LAST_CONTACT,
    LONGITUDE,
    LATITUDE,
    BARO_ALTITUDE,
    ON_GROUND,
    VELOCITY,
    TRUE_TRACK,
    VERTICAL_RATE,
    SENSORS,
    GEO_ALTITUDE,
    SQUAWK,
    SPI,
    POSITION_SOURCE,
    CATEGORY,
    LOAD_TIME
FROM raw_bronze
