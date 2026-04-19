WITH source AS (
    SELECT * FROM {{source('raw', 'section_3')}}
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
        -- -----------------------------------------------------------------

        {{case_for_number_of_days_in_month("T25")}}                              AS no_of_days_max_temp_25_c_or_higher,
        {{case_for_number_of_days_in_month("T30")}}                              AS no_of_days_max_temp_30_c_or_higher,
        {{case_for_number_of_days_in_month("T35")}}                              AS no_of_days_max_temp_35_c_or_higher,
        {{case_for_number_of_days_in_month("T40")}}                              AS no_of_days_max_temp_40_c_or_higher,

        {{case_for_number_of_days_in_month("Tn0")}}                              AS no_of_days_min_temp_lt_0_c,
        {{case_for_number_of_days_in_month("Tx0")}}                              AS no_of_days_max_temp_lt_0_c,

        {{case_for_number_of_days_in_month("R01")}}                              AS no_of_days_precip_1_0_mm_or_more,
        {{case_for_number_of_days_in_month("R05")}}                              AS no_of_days_precip_5_0_mm_or_more,
        {{case_for_number_of_days_in_month("R10")}}                              AS no_of_days_precip_10_0_mm_or_more,
        {{case_for_number_of_days_in_month("R50")}}                              AS no_of_days_precip_50_0_mm_or_more,
        {{case_for_number_of_days_in_month("R100")}}                             AS no_of_days_precip_100_0_mm_or_more,
        {{case_for_number_of_days_in_month("R150")}}                             AS no_of_days_precip_150_0_mm_or_more,

        {{case_for_number_of_days_in_month("s00")}}                              AS no_of_days_snow_depth_gt_0_cm,
        {{case_for_number_of_days_in_month("s01")}}                              AS no_of_days_snow_depth_gt_1_cm,
        {{case_for_number_of_days_in_month("s10")}}                              AS no_of_days_snow_depth_gt_10_cm,
        {{case_for_number_of_days_in_month("s50")}}                              AS no_of_days_snow_depth_gt_50_cm,

        {{case_for_number_of_days_in_month("f10")}}                              AS no_of_days_wind_speed_10_mps_or_more,
        {{case_for_number_of_days_in_month("f20")}}                              AS no_of_days_wind_speed_20_mps_or_more,
        {{case_for_number_of_days_in_month("f30")}}                              AS no_of_days_wind_speed_30_mps_or_more,

        {{case_for_number_of_days_in_month("V1")}}                               AS no_of_days_visibility_lt_50_m,
        {{case_for_number_of_days_in_month("V2")}}                               AS no_of_days_visibility_lt_100_m,
        {{case_for_number_of_days_in_month("V3")}}                               AS no_of_days_visibility_lt_1000_m

    FROM source
    WHERE TRY_CAST(source.year AS INTEGER) IS NOT NULL
          AND
          TRY_CAST(source.month AS INTEGER) IS NOT NULL
          AND
          TRY_CAST("IIiii" AS INTEGER) IS NOT NULL

)

SELECT * FROM renamed