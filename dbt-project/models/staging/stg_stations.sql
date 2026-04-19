WITH source AS (
    SELECT * FROM {{source('raw', 'stations')}}
),

renamed AS (
    SELECT
        TRY_CAST("WMO-StationID" AS INTEGER)              AS station_id,
        StationName              AS station_name,
        Country,
        CAST("Latitude" AS NUMERIC)                        AS latitude,
        CAST("Longitude" AS NUMERIC)                        AS longitude,
        CAST("Height" AS NUMERIC)                     AS height_m,
    FROM source
)

SELECT * FROM renamed
