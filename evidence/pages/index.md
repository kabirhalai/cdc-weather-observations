---
title: Analysing & Exploring Global Climate Observations (Deutscher Wetterdienst)
---

<Details title='Project Summary & Motivation'>
This project explores global climate observations from the Deutscher Wetterdienst (DWD) using SQL and Evidence's interactive components. The goal is to analyze trends in temperature, precipitation, and other climate variables over time and across different regions. By leveraging DWD's extensive dataset, we aim to uncover insights into climate patterns and changes, contributing to a better understanding of global climate dynamics.
</Details>

```sql global_mean_anomaly_by_year
select
    year,
    round(avg(temp_anomaly), 3)   as mean_temp_anomaly,
    round(min(temp_anomaly), 3)   as min_temp_anomaly,
    round(max(temp_anomaly), 3)   as max_temp_anomaly,
    count(*)                       as station_months
from warehouse.mart_climate_anomaly
where temp_anomaly is not null
group by year
order by year
```

<LineChart
    data={global_mean_anomaly_by_year}
    x=year
    y=mean_temp_anomaly
    title="Global mean temperature anomaly by year"
    yAxisTitle="Mean anomaly (°C)"
    xAxisTitle="Year"
    colorPalette={['#D85A30']}
/>

## Station data quality

This section explores the quality of the station data in terms of completeness and coverage. The data is categorized into three quality tiers: SPARSE, MEDIUM, and HIGH based on the percentage of months for which the station has fully complete data, i.e. 0 days with missing data in the month for any detail like percepitation or sunshine. SPARSE stations have less than 50% of months fully complete, MEDIUM stations have between 50% and 80% of months fully complete, and HIGH stations have more than 80% of months fully complete.

```sql quality_tier_distribution
select
    quality_tier,
    count(*) as station_count,
    round(count(*) * 100.0 / sum(count(*)) over (), 1) as pct_of_total
from warehouse.mart_station_quality
group by quality_tier
order by station_count desc
```

<BarChart
    data={quality_tier_distribution}
    x=quality_tier
    y=station_count
    title="Station count by data quality tier"
    yAxisTitle="Number of stations"
    xAxisTitle="Quality tier"
    labels=true
    colorPalette={['#1D9E75', '#EF9F27', '#D85A30']}
/>


```sql sparse_count
select
    count(*) as sparse_stations
from warehouse.mart_station_quality
where quality_tier = 'SPARSE'
```

```sql worst_stations
select
    station_id,
    coalesce(station_name, cast(station_id as varchar))  as station_name,
    coalesce("Country", 'Unknown')                      as country,
    quality_tier,
    round(percent_months_fully_complete, 1)                as pct_complete,
    total_months_reported
from warehouse.mart_station_quality q
order by percent_months_fully_complete asc
limit 20
```

<BigValue
    data={sparse_count}
    value=sparse_stations
    title="Stations with sparse data"
    subtitle="Less than 50% of months fully complete"
/>

<DataTable
    data={worst_stations}
    title="20 worst stations by data completeness"
    rows=20
>
    <Column id=station_id title="Station ID" />
    <Column id=station_name title="Station name" />
    <Column id=country title="Country" />
    <Column id=quality_tier title="Quality tier" />
    <Column id=pct_complete title="% months fully reported" fmt="0.0" />
    <Column id=total_months_reported title="Months reported" />
</DataTable>