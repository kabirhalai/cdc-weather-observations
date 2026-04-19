-- models/staging/stg_section_1.sql

WITH source AS (
    SELECT * FROM {{source('raw', 'section_1')}}
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

        -- ----------------------------------------------------------------
        -- PRESSURE
        -- Stored in tenths of hPa. Valid range: 0–1100 hPa
        -- ----------------------------------------------------------------
        CASE WHEN "Po" / 10.0 > 1100 THEN NULL
             ELSE "Po" / 10.0 END                    AS atm_station_height_hpa,

        CASE WHEN "P" / 10.0 > 1100 THEN NULL
             ELSE "P" / 10.0 END                     AS atm_sea_level_hpa,

        -- ----------------------------------------------------------------
        -- TEMPERATURE
        -- Stored in tenths of °C, sign applied via sn column
        -- sn = 0 → positive or zero, sn = 1 → negative
        -- Already NULL where missing
        -- ----------------------------------------------------------------
        CASE
            WHEN "T"  IS NULL THEN NULL
            WHEN "sn"   = 1   THEN -("T"  / 10.0)
            ELSE "T"  / 10.0
        END                                          AS temp_mean_monthly_c,

        CASE
            WHEN "Tx" IS NULL THEN NULL
            WHEN "sn.1" = 1   THEN -("Tx" / 10.0)
            ELSE "Tx" / 10.0
        END                                          AS temp_max_monthly_c,

        CASE
            WHEN "Tn" IS NULL THEN NULL
            WHEN "sn.2" = 1   THEN -("Tn" / 10.0)
            ELSE "Tn" / 10.0
        END                                          AS temp_min_monthly_c,

        -- Standard deviation of daily mean temps, tenths of °C
        CASE WHEN "st" IS NULL THEN NULL
             ELSE "st" / 10.0 END                    AS temp_daily_stddev_c,

        -- ----------------------------------------------------------------
        -- VAPOUR PRESSURE
        -- Stored in tenths of hPa. Valid range: 0–70 hPa
        -- Already NULL where missing
        -- ----------------------------------------------------------------
        CASE WHEN "e" / 10.0 > 70 THEN NULL
             ELSE "e" / 10.0 END                     AS vapor_pressure_mean_hpa,

        -- ----------------------------------------------------------------
        -- PRECIPITATION
        -- Already in mm (not tenths). Sentinel is 9999.
        -- ----------------------------------------------------------------
        CASE WHEN "R1" >= 9999 THEN NULL
             ELSE "R1" END                           AS precip_monthly_mm,

        -- Quintile group 1–5. Values > 5 are sentinels.
        CASE WHEN "Rd" > 5 THEN NULL
             ELSE CAST("Rd" AS INTEGER) END          AS precip_quintile,

        -- Days with >= 1mm precipitation. Valid range: 0–31.
        CASE WHEN "nr" > 31 THEN NULL
             ELSE CAST("nr" AS INTEGER) END          AS rainy_days_count,

        -- ----------------------------------------------------------------
        -- SUNSHINE
        -- S1 in full hours. Valid max: 744 hrs (31 days × 24 hrs).
        -- ps is integer percentage 0–100. Values > 100 are sentinels.
        -- ----------------------------------------------------------------
        CASE WHEN "S1" > 744 THEN NULL
             ELSE "S1" END                           AS sunshine_duration_hours,

        CASE WHEN "ps" > 100 THEN NULL
             ELSE CAST("ps" AS INTEGER) END          AS sunshine_pct_of_normal,

        -- ----------------------------------------------------------------
        -- DATA QUALITY FLAGS (days missing from record)
        -- ----------------------------------------------------------------
        CAST("mp"  AS INTEGER)                       AS missing_days_pressure,
        CAST("mT"  AS INTEGER)                       AS missing_days_temp,
        CAST("mTx" AS INTEGER)                       AS missing_days_temp_max,
        CAST("mTn" AS INTEGER)                       AS missing_days_temp_min,
        CAST("me"  AS INTEGER)                       AS missing_days_vapor_pressure,
        CAST("mR"  AS INTEGER)                       AS missing_days_precip,
        CAST("mS"  AS INTEGER)                       AS missing_days_sunshine

        -- G1, G1.1 ... G1.8 intentionally dropped (CLIMAT format group markers)

    FROM source
    WHERE TRY_CAST(source.year AS INTEGER) IS NOT NULL
          AND
          TRY_CAST(source.month AS INTEGER) IS NOT NULL
          AND
          TRY_CAST("IIiii" AS INTEGER) IS NOT NULL
)

SELECT * FROM renamed