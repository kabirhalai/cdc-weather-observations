WITH source AS (
    SELECT * FROM {{source('raw', 'section_4')}}
),

renamed AS (
    SELECT
        -- ----------------------------------------------------------------
        -- IDENTIFIERS
        -- ----------------------------------------------------------------
        TRY_CAST(source.year AS INTEGER)                        AS year,
        TRY_CAST(source.month AS INTEGER)                       AS month,

        -- Station ID
        TRY_CAST("IIiii" AS INTEGER)                     AS station_id,


        {{case_for_number_of_days_in_month("Dts")}}                              AS no_of_days_thunderstorms,
        {{case_for_number_of_days_in_month("Dgr")}}                              AS no_of_days_hail
    FROM source
    WHERE TRY_CAST(source.year AS INTEGER) IS NOT NULL
          AND
          TRY_CAST(source.month AS INTEGER) IS NOT NULL
          AND
          TRY_CAST("IIiii" AS INTEGER) IS NOT NULL

)

select * from renamed
