---
title: Analysing & Exploring Global Climate Observations (Deutscher Wetterdienst)
---

<Details title='Project Summary & Motivation'>
This project explores global climate observations from the Deutscher Wetterdienst (DWD) using SQL and Evidence's interactive components. The goal is to analyze trends in temperature, precipitation, and other climate variables over time and across different regions. By leveraging DWD's extensive dataset, we aim to uncover insights into climate patterns and changes, contributing to a better understanding of global climate dynamics.
</Details>

```sql countries
  select
      Country
  from warehouse.stations
  group by Country
```

```sql stations
  select
      station_name
  from warehouse.stations
  where Country = '${inputs.selected_country.value}'
```
<Dropdown data={countries} name=selected_country value=Country>
    <DropdownOption value="%" valueLabel="All Countries"/>
</Dropdown>

<Dropdown data={stations} name=selected_station value=station_name>
    <DropdownOption value="%" valueLabel="Station Name"/>
</Dropdown>


```sql highest_anomaly
select
    a.station_id,
    a.year,
    a.month,
    round(a.temp_anomaly, 2) as temp_anomaly,
    coalesce(s.station_name, cast(a.station_id as varchar)) as station_name
from warehouse.mart_climate_anomaly a
left join warehouse.stations s
    on a.station_id = s.station_id
order by abs(a.temp_anomaly) desc
limit 1
```

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