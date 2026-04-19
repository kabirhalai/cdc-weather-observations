WITH stg_section1 AS (
    SELECT * FROM {{ref('stg_section1')}}
),
stg_section2 AS (
    SELECT * FROM {{ref('stg_section2')}}
),
stg_section3 AS (
    SELECT * FROM {{ref('stg_section3')}}
),
stg_section4 AS (
    SELECT * FROM {{ref('stg_section4')}}
),

combined1 AS (
    SELECT *
    FROM 
    stg_section1 LEFT JOIN stg_section2 USING ("year", "month", "station_id")
),

combined2 AS (
    SELECT *
    FROM combined1 LEFT JOIN stg_section3 USING ("year", "month", "station_id")
),

final_combined AS (
    SELECT *
    FROM combined2 LEFT JOIN stg_section4 USING ("year", "month", "station_id")
)

SELECT * FROM final_combined