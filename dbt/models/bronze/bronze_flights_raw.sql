{{
    config(materialized='view', tags=['bronze', 'source'])
}}

-- View on top of raw Bronze table
-- Parses JSON and extracts timestamps

SELECT
    INGESTION_TIME,
    RAW_DATA:icao24::VARCHAR AS icao24,
    RAW_DATA:origin_country::VARCHAR AS origin_country,
    RAW_DATA:latitude::FLOAT AS latitude,
    RAW_DATA:longitude::FLOAT AS longitude,
    RAW_DATA:time_position::TIMESTAMP AS time_position,
    RAW_DATA:last_contact::TIMESTAMP AS last_contact,
    RAW_DATA:velocity::FLOAT AS velocity,
    RAW_DATA:vertical_rate::FLOAT AS vertical_rate,
    RAW_DATA:true_track::FLOAT AS true_track,
    RAW_DATA:baro_altitude::FLOAT AS baro_altitude,
    RAW_DATA:geo_altitude::FLOAT AS geo_altitude,
    RAW_DATA:on_ground::BOOLEAN AS on_ground,
    SOURCE_FILE,
    INGESTION_BATCH

FROM {{source('bronze', 'BRONZE_FLIGHTS')}}
WHERE RAW_DATA IS NOT NULL