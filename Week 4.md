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


### Question 6: P97/P95/P90 Taxi Monthly Fare

1. Create a new model `fct_taxi_trips_monthly_fare_p95.sql`
2. Filter out invalid entries (`fare_amount > 0`, `trip_distance > 0`, and `payment_type_description in ('Cash', 'Credit Card')`)
3. Compute the **continous percentile** of `fare_amount` partitioning by service_type, year and and month

Now, what are the values of `p97`, `p95`, `p90` for Green Taxi and Yellow Taxi, in April 2020?

```sql
-- Using the below as `fct_taxi_trips_monthly_fare_p95.sql`

{{
    config(
        materialized='table'
    )
}}

WITH filtered_data AS (
    SELECT
        service_type,
        fare_amount
    FROM {{ ref('fct_taxi_trips') }}
    WHERE
        fare_amount > 0
        AND trip_distance > 0
        AND payment_type_description IN ('Cash', 'Credit Card')
        AND year_p = 2020
        AND month_p = 4
),
percentiles AS (
    SELECT
        service_type,
        PERCENTILE_CONT(fare_amount, 0.97) OVER (PARTITION BY service_type) AS p97_fare_amount,
        PERCENTILE_CONT(fare_amount, 0.95) OVER (PARTITION BY service_type) AS p95_fare_amount,
        PERCENTILE_CONT(fare_amount, 0.90) OVER (PARTITION BY service_type) AS p90_fare_amount
    FROM filtered_data
)
SELECT
    *
FROM
    percentiles
GROUP BY
    service_type, p97_fare_amount, p95_fare_amount, p90_fare_amount
ORDER BY
    service_type
```
Amswer:
Row	               Green          Yellow
service_type       28.0           32.0
p97_fare_amount    23.0           26.0   
p95_fare_amount    18.0           19.5
p90_fare_amount

- green: {p97: 40.0, p95: 33.0, p90: 24.5}, yellow: {p97: 31.5, p95: 25.5, p90: 19.0}

### Question 7: Top #Nth longest P90 travel time Location for FHV

Prerequisites:
* Create a staging model for FHV Data (2019), and **DO NOT** add a deduplication step, just filter out the entries where `where dispatching_base_num is not null`
* Create a core model for FHV Data (`dim_fhv_trips.sql`) joining with `dim_zones`. Similar to what has been done [here](../../../04-analytics-engineering/taxi_rides_ny/models/core/fact_trips.sql)
* Add some new dimensions `year` (e.g.: 2019) and `month` (e.g.: 1, 2, ..., 12), based on `pickup_datetime`, to the core model to facilitate filtering for your queries

Now...
1. Create a new model `fct_fhv_monthly_zone_traveltime_p90.sql`
2. For each record in `dim_fhv_trips.sql`, compute the [timestamp_diff](https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#timestamp_diff) in seconds between dropoff_datetime and pickup_datetime - we'll call it `trip_duration` for this exercise
3. Compute the **continous** `p90` of `trip_duration` partitioning by year, month, pickup_location_id, and dropoff_location_id

Answer, a table was created using year as the partition and month, pickup_location_id, and dropoff_location_id as clusters

For the Trips that **respectively** started from `Newark Airport`, `SoHo`, and `Yorkville East`, in November 2019, what are **dropoff_zones** with the 2nd longest p90 trip_duration ?
After creating a partition using years
```sql
SELECT
    year,
    month,
    pickup_locationid,
    dropoff_locationid,
    pickup_zone,
    dropoff_zone,
    APPROX_QUANTILES(trip_duration, 100)[OFFSET(90)] AS trip_duration_p90
FROM
    `global-rookery-448215-m8.dbt_oojekunbi.dim_fhv_trips_partitioned_clustered`
WHERE month = 11
  AND year = 2019
  AND pickup_zone in ('Newark Airport', 'Yorkville East', 'SoHo')
GROUP BY
    year,
    month,
    pickup_locationid,
    dropoff_locationid,
    pickup_zone,
    dropoff_zone
```
Answer - LaGuardia Airport, Greenwich Village South, Garment District







