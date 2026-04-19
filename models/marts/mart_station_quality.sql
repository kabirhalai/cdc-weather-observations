WITH observations AS (
    SELECT * FROM {{ ref('int_observations_combined') }}
),

quality_scores AS (
    SELECT
        station_id,
        COUNT(month)                   AS total_months_reported,
        AVG(missing_days_temp)               AS avg_missing_temp_days,
        AVG(missing_days_precip)              AS avg_missing_days_precip,
        AVG(missing_days_sunshine)         AS avg_missing_days_sunshine,
        COUNT(
            CASE WHEN COALESCE(missing_days_sunshine, 1) = 0
                  AND COALESCE(missing_days_precip, 1) = 0
                  AND COALESCE(missing_days_vapor_pressure, 1) = 0
                  AND COALESCE(missing_days_temp_min, 1) = 0
                  AND COALESCE(missing_days_temp_max, 1) = 0
                  AND COALESCE(missing_days_temp, 1) = 0
                  AND COALESCE(missing_days_pressure, 1) = 0
            THEN 1 END
        ) * 100.0 / COUNT(*)     AS percent_months_fully_complete
    FROM observations
    GROUP BY station_id
),

stations AS (
    SELECT * FROM {{ ref('stg_stations') }}
)

SELECT
    quality_scores.*,
    stations.station_name,
    stations.Country,
    CASE
        WHEN percent_months_fully_complete >= 80 THEN 'HIGH'
        WHEN percent_months_fully_complete >= 50 THEN 'MEDIUM'
        ELSE 'SPARSE'
    END AS quality_tier
FROM quality_scores LEFT JOIN stations USING ("station_id")



