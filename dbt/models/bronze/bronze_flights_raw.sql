{{
    config(materialized='view', tags=['bronze', 'source'])
}}

-- View on top of raw Bronze table.
-- The OpenSky API returns states as a list of arrays (positional fields, not named keys).
-- We use LATERAL FLATTEN to explode each state row, then access fields by array index.
-- OpenSky state vector field order (17 elements):
--   [0] icao24, [1] callsign, [2] origin_country, [3] time_position, [4] last_contact,
--   [5] longitude, [6] latitude, [7] velocity, [8] on_ground, [9] true_track,
--   [10] vertical_rate, [11] sensors, [12] baro_altitude, [13] squawk,
--   [14] spi, [15] position_source, [16] geo_altitude

SELECT
    src.INGESTION_TIME,
    f.value[0]::VARCHAR          AS ICAO24,
    f.value[2]::VARCHAR          AS ORIGIN_COUNTRY,
    f.value[6]::FLOAT            AS LATITUDE,
    f.value[5]::FLOAT            AS LONGITUDE,
    TO_TIMESTAMP(f.value[3]::INTEGER) AS TIME_POSITION,
    TO_TIMESTAMP(f.value[4]::INTEGER) AS LAST_CONTACT,
    f.value[7]::FLOAT            AS VELOCITY,
    f.value[10]::FLOAT           AS VERTICAL_RATE,
    f.value[9]::FLOAT            AS TRUE_TRACK,
    f.value[12]::FLOAT           AS BARO_ALTITUDE,
    f.value[16]::FLOAT           AS GEO_ALTITUDE,
    f.value[8]::BOOLEAN          AS ON_GROUND,
    src.SOURCE_FILE,
    src.INGESTION_BATCH

FROM {{source('bronze', 'BRONZE_FLIGHTS')}} src,
LATERAL FLATTEN(input => src.RAW_DATA:states) f
WHERE src.RAW_DATA IS NOT NULL
  AND src.RAW_DATA:states IS NOT NULL