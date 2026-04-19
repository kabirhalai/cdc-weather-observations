WITH observations AS (
    SELECT * FROM {{ ref('int_observations_combined') }}
),
stations AS (
    SELECT * FROM {{ ref('stg_stations') }}
),

joined_data AS (
	SELECT
		"year",
		"month",
		station_id,
		atm_station_height_hpa,
		atm_sea_level_hpa,
		temp_mean_monthly_c,
		temp_max_monthly_c,
		temp_min_monthly_c,
		temp_daily_stddev_c,
		vapor_pressure_mean_hpa,
		precip_monthly_mm,
		precip_quintile,
		rainy_days_count,
		sunshine_duration_hours,
		sunshine_pct_of_normal,
		missing_days_pressure,
		missing_days_temp,
		missing_days_temp_max,
		missing_days_temp_min,
		missing_days_vapor_pressure,
		missing_days_precip,
		missing_days_sunshine,
		stations.station_name,
		stations.country,
		stations.latitude,
		stations.longitude

	FROM observations LEFT JOIN stations USING ("station_id")
)

select * from joined_data