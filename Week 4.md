Week 4 homework.

### Question 1: Understanding dbt model resolution
```yaml
version: 2

sources:
  - name: raw_nyc_tripdata
    database: "{{ env_var('DBT_BIGQUERY_PROJECT', 'dtc_zoomcamp_2025') }}"
    schema:   "{{ env_var('DBT_BIGQUERY_SOURCE_DATASET', 'raw_nyc_tripdata') }}"
    tables:
      - name: ext_green_taxi
      - name: ext_yellow_taxi
```
The above means the database env variable for defaults.


```shell
export DBT_BIGQUERY_PROJECT=myproject
export DBT_BIGQUERY_DATASET=my_nyc_tripdata
```
This means that the DBT database nme was set.


```sql
select * 
from {{ source('raw_nyc_tripdata', 'ext_green_taxi' ) }}
```
Answer is - `select * from myproject.my_nyc_tripdata.ext_green_taxi`

### Question 2: dbt Variables & Dynamic Models

Say you have to modify the following dbt_model (`fct_recent_taxi_trips.sql`) to enable Analytics Engineers to dynamically control the date range. 

- In development, you want to process only **the last 7 days of trips**
- In production, you need to process **the last 30 days** for analytics

```sql
select *
from {{ ref('fact_taxi_trips') }}
where pickup_datetime >= CURRENT_DATE - INTERVAL '30' DAY
```

What would you change to accomplish that in a such way that command line arguments takes precedence over ENV_VARs, which takes precedence over DEFAULT value?
Answer is - Update the WHERE clause to `pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY`

In this case, the var for the development while the env var for the production.


### Question 3: dbt Data Lineage and Execution

Select the option that does **NOT** apply for materializing `fct_taxi_monthly_zone_revenue`:

Answer is - `dbt run --select models/staging/+` This will only run for the staging directory and not the core.



### Question 4: dbt Macros and Jinja

```sql
{% macro resolve_schema_for(model_type) -%}

    {%- set target_env_var = 'DBT_BIGQUERY_TARGET_DATASET'  -%}
    {%- set stging_env_var = 'DBT_BIGQUERY_STAGING_DATASET' -%}

    {%- if model_type == 'core' -%} {{- env_var(target_env_var) -}}
    {%- else -%}                    {{- env_var(stging_env_var, env_var(target_env_var)) -}}
    {%- endif -%}

{%- endmacro %}
```

And use on your staging, dim_ and fact_ models as:
```sql
{{ config(
    schema=resolve_schema_for('core'), 
) }}
```

That all being said, regarding macro above, **select all statements that are true to the models using it**:
Answer:
- When using `core`, it materializes in the dataset defined in `DBT_BIGQUERY_TARGET_DATASET`
- When using `stg`, it materializes in the dataset defined in `DBT_BIGQUERY_STAGING_DATASET`, or defaults to `DBT_BIGQUERY_TARGET_DATASET`
- When using `staging`, it materializes in the dataset defined in `DBT_BIGQUERY_STAGING_DATASET`, or defaults to `DBT_BIGQUERY_TARGET_DATASET`


You might want to add some new dimensions `year` (e.g.: 2019, 2020), `quarter` (1, 2, 3, 4), `year_quarter` (e.g.: `2019/Q1`, `2019-Q2`), and `month` (e.g.: 1, 2, ..., 12), **extracted from pickup_datetime**, to your `fct_taxi_trips` OR `dim_taxi_trips.sql` models to facilitate filtering your queries

This is my `fct_taxi_trips`
```sql
{{
    config(
        materialized='table'
    )
}}

with green_tripdata as (
    select *, 
        'Green' as service_type
    from {{ ref('stg_green_tripdata') }}
), 
yellow_tripdata as (
    select *, 
        'Yellow' as service_type
    from {{ ref('stg_yellow_tripdata') }}
), 
trips_unioned as (
    select * from green_tripdata
    union all 
    select * from yellow_tripdata
), 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select trips_unioned.tripid, 
    trips_unioned.vendorid, 
    trips_unioned.service_type,
    trips_unioned.ratecodeid, 
    trips_unioned.pickup_locationid, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    trips_unioned.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    trips_unioned.pickup_datetime,
    EXTRACT(YEAR FROM trips_unioned.pickup_datetime) AS year_p,
    EXTRACT(QUARTER FROM pickup_datetime) AS quarter,
    EXTRACT(MONTH FROM pickup_datetime) AS month_p,
    FORMAT_DATE('%Y/Q%Q', DATE_TRUNC(pickup_datetime, QUARTER)) AS year_quarter,
    trips_unioned.dropoff_datetime, 
    trips_unioned.store_and_fwd_flag, 
    trips_unioned.passenger_count, 
    trips_unioned.trip_distance, 
    trips_unioned.trip_type, 
    trips_unioned.fare_amount, 
    trips_unioned.extra, 
    trips_unioned.mta_tax, 
    trips_unioned.tip_amount, 
    trips_unioned.tolls_amount, 
    trips_unioned.ehail_fee, 
    trips_unioned.improvement_surcharge, 
    trips_unioned.total_amount, 
    trips_unioned.payment_type, 
    trips_unioned.payment_type_description
from trips_unioned
inner join dim_zones as pickup_zone
on trips_unioned.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on trips_unioned.dropoff_locationid = dropoff_zone.locationid
```

### Question 5: Taxi Quarterly Revenue Growth

1. Create a new model `fct_taxi_trips_quarterly_revenue.sql`
2. Compute the Quarterly Revenues for each year for based on `total_amount`
3. Compute the Quarterly YoY (Year-over-Year) revenue growth 
  * e.g.: In 2020/Q1, Green Taxi had -12.34% revenue growth compared to 2019/Q1
  * e.g.: In 2020/Q4, Yellow Taxi had +34.56% revenue growth compared to 2019/Q4

Considering the YoY Growth in 2020, which were the yearly quarters with the best (or less worse) and worst results for green, and yellow

Here is the `fct_taxi_trips_quarterly_revenue.sql`
```sql
{{
    config(
        materialized='table'
    )
}}

WITH quarterly_revenue AS (
    SELECT
        -- Revenue grouping
        quarter, 
        year_quarter AS quarterly_revenues, 
        service_type,
        -- Revenue calculation 
        SUM(total_amount) AS revenue_quarterly_total_amount
    FROM {{ ref('fct_taxi_trips') }}
    WHERE year_quarter BETWEEN '2019/Q1' AND '2020/Q4'
    GROUP BY quarter, year_quarter, service_type
    ORDER BY quarter ASC, year_quarter, service_type ASC
),

quarterly_revenue_with_lag AS (
    SELECT
        quarterly_revenues,
        service_type,
        revenue_quarterly_total_amount,
        LAG(revenue_quarterly_total_amount, 4, 0) OVER (PARTITION BY service_type ORDER BY quarterly_revenues) AS previous_year_revenue  -- Look back 4 quarters
    FROM quarterly_revenue
),

quarterly_revenue_growth AS (
    SELECT
        quarterly_revenues,
        service_type,
        revenue_quarterly_total_amount,
        previous_year_revenue,
        (revenue_quarterly_total_amount - previous_year_revenue) * 100.0 / NULLIF(previous_year_revenue, 0) AS revenue_growth_percentage -- Calculate percentage growth
    FROM quarterly_revenue_with_lag
)

SELECT * FROM quarterly_revenue_growth
```
Answer is - green: {best: 2020/Q2, worst: 2020/Q1}, yellow: {best: 2020/Q2, worst: 2020/Q1}








