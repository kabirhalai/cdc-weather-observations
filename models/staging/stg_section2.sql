WITH source AS (
    SELECT * FROM {{source('raw', 'section_2')}}
),

renamed AS (
    SELECT
        -- ----------------------------------------------------------------
        -- IDENTIFIERS
        -- ----------------------------------------------------------------
        TRY_CAST(source.year AS INTEGER)                 AS year,
        TRY_CAST(source.month AS INTEGER)                AS month,

        -- Station ID
        TRY_CAST(source."IIiii" AS INTEGER)              AS station_id,

        CASE WHEN source."Po.1" / 10.0 > 1100 THEN NULL
             ELSE source."Po.1" / 10.0 END                 AS atm_station_height_hpa,

        CASE WHEN source."P.1" / 10.0 > 1100 THEN NULL
             ELSE source."P.1" / 10.0 END                  AS atm_sea_level_hpa,


        {{ apply_sign_and_adjust_tenths('"T.1"', '"sn.3"') }} AS temp_mean_monthly_c,
        {{ apply_sign_and_adjust_tenths('"Tx.1"', '"sn.4"') }} AS max_temp_mean_monthly_c,
        {{ apply_sign_and_adjust_tenths('"Tn.1"', '"sn.5"') }} AS min_temp_mean_monthly_c,

        {{ case_for_number_of_days_in_month('"nr.1"') }} AS no_of_days_with_precipitation_1_mm_or_more
        
    FROM source
    WHERE TRY_CAST(source.year AS INTEGER) IS NOT NULL
          AND
          TRY_CAST(source.month AS INTEGER) IS NOT NULL
          AND
          TRY_CAST("IIiii" AS INTEGER) IS NOT NULL

)

SELECT * FROM renamed