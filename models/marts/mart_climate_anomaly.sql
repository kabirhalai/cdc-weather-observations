WITH monthly_observations AS (
    SELECT * FROM {{ ref('mart_monthly_observations') }}
),
long_term_averages AS (
    SELECT
        "year",
        "month",
        station_id,
        reference_period_start_year,
        reference_period_end_year,
        temp_std_dev_daily_mean_c,
        atm_station_height_hpa_long_term_average,
        atm_sea_level_hpa_long_term_average,
        vapour_pressure_mean_hpa,
        precipitation_total_mm,
        sunshine_total_hours,
        temp_mean_monthly_c_long_term_average,
        max_temp_mean_monthly_c,
        min_temp_mean_monthly_c,
    FROM {{ ref('int_observations_combined') }}
)

SELECT
    mo.year,
    mo.month,
    mo.station_id,
    mo.temp_mean_monthly_c - temp_mean_monthly_c_long_term_average as temp_anomaly,
    abs(temp_anomaly) > (3 * lta.temp_std_dev_daily_mean_c) AS is_significant_anomaly,
    
FROM monthly_observations mo LEFT JOIN long_term_averages lta USING (station_id, month)