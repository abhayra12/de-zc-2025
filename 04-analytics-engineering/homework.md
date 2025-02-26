# Module 4 Homework

For this homework, you will need the following datasets:

- Green Taxi dataset (2019 and 2020): 7,778,101 records
- Yellow Taxi dataset (2019 and 2020): 109,047,518 records
- For Hire Vehicle dataset (2019): 43,244,696 records

**Preparing dataset for FHV**

- Upload csv's files into a bucket

- Create final table for FHV in BigQuery

```sql

CREATE TABLE `eastern-amp-449614-e1.trips_data_all.FHV_2019`
(
  dispatching_base_num STRING,
  pickup_datetime TIMESTAMP,
  dropOff_datetime TIMESTAMP,
  PUlocationID INT64,
  DOlocationID INT64,
  SR_Flag STRING,
  Affiliated_base_number STRING

)

```

- Create external table for FHV 2019 January in BigQuery:


```

CREATE OR REPLACE EXTERNAL TABLE `eastern-amp-449614-e1.trips_data_all.FHV_2019_01`
(
  dispatching_base_num STRING,
  pickup_datetime TIMESTAMP,
  dropOff_datetime TIMESTAMP,
  PUlocationID INT64,
  DOlocationID INT64,
  SR_Flag STRING,
  Affiliated_base_number STRING
)
OPTIONS (
    format = 'parquet',
    uris = ['gs://fhv_taxi_gcs/fhv/fhv_tripdata_2019-01.parquet'],
    skip_leading_rows = 1,
    ignore_unknown_values = TRUE
      );

```      

- Insert values from January external table into final table:

```

INSERT INTO `eastern-amp-449614-e1.trips_data_all.FHV_2019`
(dispatching_base_num, pickup_datetime, dropOff_datetime, PUlocationID, DOlocationID, SR_Flag, Affiliated_base_number)
SELECT dispatching_base_num, pickup_datetime, dropOff_datetime, PUlocationID, DOlocationID, SR_Flag, Affiliated_base_number
FROM `eastern-amp-449614-e1.trips_data_all.FHV_2019_01`;
```

- Repeat for remaining months


## Question 1: Understanding dbt model resolution

Provided you've got the following sources.yaml:

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

with the following env variables setup where `dbt` runs:

```bash
export DBT_BIGQUERY_PROJECT=myproject
export DBT_BIGQUERY_DATASET=my_nyc_tripdata
```

What does this .sql model compile to?

```sql

select * 
from {{ source('raw_nyc_tripdata', 'ext_green_taxi' ) }}
```

**Resolution:**

- env_var('DBT_BIGQUERY_PROJECT', 'dtc_zoomcamp_2025') resolves to myproject.
- env_var('DBT_BIGQUERY_SOURCE_DATASET', 'raw_nyc_tripdata') resolves to raw_nyc_tripdata.

source('raw_nyc_tripdata', 'ext_green_taxi') will resolve to:

```myproject.raw_nyc_tripdata.ext_green_taxi```

The .sql model compiles to:

**```select * from `myproject.raw_nyc_tripdata.ext_green_taxi` ```**


## Question 2: dbt Variables & Dynamic Models

Say you have to modify the following dbt_model (`fct_recent_taxi_trips.sql`) to enable Analytics Engineers to dynamically control the date range. 

- In development, you want to process only **the last 7 days of trips**
- In production, you need to process **the last 30 days** for analytics

```sql

select *
from {{ ref('fact_taxi_trips') }}
where pickup_datetime >= CURRENT_DATE - INTERVAL '30' DAY

```

What would you change to accomplish that in a such way that command line arguments takes precedence 
over ENV_VARs, which takes precedence over DEFAULT value?

```sql

select *
from {{ ref('fact_taxi_trips') }}
where pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", 30)) }}' DAY

```


The code demonstrates a dbt variable priority hierarchy: command line arguments > environment variables > default value of 30 days.


## Question 3: dbt Data Lineage and Execution

Considering the data lineage below **and** that taxi_zone_lookup is the **only** materialization build (from a .csv seed file):

![image](./homework_q2.png)

Select the option that does **NOT** apply for materializing `fct_taxi_monthly_zone_revenue`:

- dbt run: This runs all models in the project. **VALID**

- dbt run --select +models/core/dim_taxi_trips.sql+ --target prod: This runs dim_taxi_trips and any dependent models. fct_taxi_monthly_zone_revenue depends on dim_taxi_trips. **VALID**

- dbt run --select +models/core/fct_taxi_monthly_zone_revenue.sql: This runs fct_taxi_monthly_zone_revenue and all the models it depends on. **VALID**

- dbt run --select +models/core/: This runs all models within models/core/, including fct_taxi_monthly_zone_revenue. **VALID**

**- dbt run --select models/staging/+: This only runs models in models/staging/ and their dependencies, but not necessarily fct_taxi_monthly_zone_revenue, as it is located in models/core/ .**


## Question 4: dbt Macros and Jinja

Consider you're dealing with sensitive data (e.g.: [PII](https://en.wikipedia.org/wiki/Personal_data)), that is **only available to your team and very selected few individuals**, in the `raw layer` of your DWH (e.g: a specific BigQuery dataset or PostgreSQL schema), 

 - Among other things, you decide to obfuscate/masquerade that data through your staging models, and make it available in a different schema (a `staging layer`) for other Data/Analytics Engineers to explore

- And **optionally**, yet  another layer (`service layer`), where you'll build your dimension (`dim_`) and fact (`fct_`) tables (assuming the [Star Schema dimensional modeling](https://www.databricks.com/glossary/star-schema)) for Dashboarding and for Tech Product Owners/Managers

You decide to make a macro to wrap a logic around it:

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
- Setting a value for  `DBT_BIGQUERY_TARGET_DATASET` env var is mandatory, or it'll fail to compile **TRUE**
- Setting a value for `DBT_BIGQUERY_STAGING_DATASET` env var is mandatory, or it'll fail to compile **FALSE**
- When using `core`, it materializes in the dataset defined in `DBT_BIGQUERY_TARGET_DATASET` **TRUE**
- When using `stg`, it materializes in the dataset defined in `DBT_BIGQUERY_STAGING_DATASET`, or defaults to `DBT_BIGQUERY_TARGET_DATASET` **TRUE**
- When using `staging`, it materializes in the dataset defined in `DBT_BIGQUERY_STAGING_DATASET`, or defaults to `DBT_BIGQUERY_TARGET_DATASET` **TRUE**


## Serious SQL

Alright, in module 1, you had a SQL refresher, so now let's build on top of that with some serious SQL.

These are not meant to be easy - but they'll boost your SQL and Analytics skills to the next level.  
So, without any further do, let's get started...

You might want to add some new dimensions `year` (e.g.: 2019, 2020), `quarter` (1, 2, 3, 4), `year_quarter` (e.g.: `2019/Q1`, `2019-Q2`), and `month` (e.g.: 1, 2, ..., 12), **extracted from pickup_datetime**, to your `fct_taxi_trips` OR `dim_taxi_trips.sql` models to facilitate filtering your queries.


## Question 5: Taxi Quarterly Revenue Growth

1. Create a new model `fct_taxi_trips_quarterly_revenue.sql`
2. Compute the Quarterly Revenues for each year for based on `total_amount`
3. Compute the Quarterly YoY (Year-over-Year) revenue growth 
  * e.g.: In 2020/Q1, Green Taxi had -12.34% revenue growth compared to 2019/Q1
  * e.g.: In 2020/Q4, Yellow Taxi had +34.56% revenue growth compared to 2019/Q4

Considering the YoY Growth in 2020, which were the yearly quarters with the best (or less worse) and worst results for green, and yellow

- green: {best: 2020/Q2, worst: 2020/Q1}, yellow: {best: 2020/Q2, worst: 2020/Q1}
- green: {best: 2020/Q2, worst: 2020/Q1}, yellow: {best: 2020/Q3, worst: 2020/Q4}
- green: {best: 2020/Q1, worst: 2020/Q2}, yellow: {best: 2020/Q2, worst: 2020/Q1}
- green: {best: 2020/Q1, worst: 2020/Q2}, yellow: {best: 2020/Q1, worst: 2020/Q2}
- green: {best: 2020/Q1, worst: 2020/Q2}, yellow: {best: 2020/Q3, worst: 2020/Q4}

Head over to dbt and create a new file under core models "fct_taxi_trips_quarterly_revenue.sql": 

```sql

{{ config(materialized='table') }}


with quarterly_revenue as (
    SELECT
        service_type,
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(QUARTER FROM pickup_datetime) AS quarter,
        SUM(total_amount) AS revenue

    FROM {{ ref('fact_trips') }}
    WHERE EXTRACT(YEAR FROM pickup_datetime) IN (2019, 2020)
    GROUP BY service_type,year,quarter
),

quarterly_growth AS (
    SELECT 
        year,
        quarter,
        service_type,
        revenue,
        LAG(revenue) OVER (PARTITION BY service_type, quarter ORDER BY year) AS prev_year_revenue,
        (revenue - LAG(revenue) OVER (PARTITION BY service_type, quarter ORDER BY year)) / 
        NULLIF(LAG(revenue) OVER (PARTITION BY service_type, quarter ORDER BY year), 0) AS yoy_growth
    FROM quarterly_revenue
)
SELECT * FROM quarterly_growth

```

run:

```
dbt build --select +fct_taxi_trips_quarterly_revenue.sql+ --vars '{is_test_run: false}'
```


Check BigQuery:

<br>

![ae63](images/ae63.jpg)
<br>


**Answer: green: {best: 2020/Q1, worst: 2020/Q2}, yellow: {best: 2020/Q1, worst: 2020/Q2}**


## Question 6: P97/P95/P90 Taxi Monthly Fare

1. Create a new model `fct_taxi_trips_monthly_fare_p95.sql`
2. Filter out invalid entries (`fare_amount > 0`, `trip_distance > 0`, and `payment_type_description in ('Cash', 'Credit Card')`)
3. Compute the **continous percentile** of `fare_amount` partitioning by service_type, year and and month

Now, what are the values of `p97`, `p95`, `p90` for Green Taxi and Yellow Taxi, in April 2020?

- green: {p97: 55.0, p95: 45.0, p90: 26.5}, yellow: {p97: 52.0, p95: 37.0, p90: 25.5}
- green: {p97: 55.0, p95: 45.0, p90: 26.5}, yellow: {p97: 31.5, p95: 25.5, p90: 19.0}
- green: {p97: 40.0, p95: 33.0, p90: 24.5}, yellow: {p97: 52.0, p95: 37.0, p90: 25.5}
- green: {p97: 40.0, p95: 33.0, p90: 24.5}, yellow: {p97: 31.5, p95: 25.5, p90: 19.0}
- green: {p97: 55.0, p95: 45.0, p90: 26.5}, yellow: {p97: 52.0, p95: 25.5, p90: 19.0}


Head over to dbt and create a new file under core models "fct_taxi_trips_monthly_fare_p95.sql"

**fct_taxi_trips_monthly_fare_p95.sql:**

```sql

{{ config(materialized='table') }}

WITH valid_trips AS (
    SELECT
        service_type,
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(MONTH FROM pickup_datetime) AS month,
        fare_amount

    FROM {{ ref('fact_trips') }}
    WHERE 
        fare_amount > 0
        AND trip_distance > 0
        AND payment_type_description IN ('Cash', 'Credit card')
),

percentiles AS (
    SELECT 
        service_type,
        year,
        month,
        PERCENTILE_CONT(fare_amount, 0.97) OVER (PARTITION BY service_type, year, month) AS p97,
        PERCENTILE_CONT(fare_amount, 0.95) OVER (PARTITION BY service_type, year, month) AS p95,
        PERCENTILE_CONT(fare_amount, 0.90) OVER (PARTITION BY service_type, year, month) AS p90
    FROM valid_trips
    
)

SELECT * FROM percentiles

```

**dbt build:**

run:

```
dbt build --select +fct_taxi_trips_monthly_fare_p95.sql+ --vars '{is_test_run: false}'
```

**Query:**

```sql

SELECT DISTINCT service_type, year, month, p97, p95, p90 
FROM `eastern-amp-449614-e1.dbt_aahirkar.fct_taxi_trips_monthly_fare_p95`
WHERE month = 4 AND year = 2020;
```


**Check results:**

<br>

![ae64](images/ae64.jpg)
<br>

**Answer: green: {p97: 55.0, p95: 45.0, p90: 26.5}, yellow: {p97: 31.5, p95: 25.5, p90: 19.0}**


## Question 7: Top #Nth longest P90 travel time Location for FHV

Prerequisites:
* Create a staging model for FHV Data (2019), and **DO NOT** add a deduplication step, just filter out the entries where `where dispatching_base_num is not null`
* Create a core model for FHV Data (`dim_fhv_trips.sql`) joining with `dim_zones`. Similar to what has been done [here](../../../04-analytics-engineering/taxi_rides_ny/models/core/fact_trips.sql)
* Add some new dimensions `year` (e.g.: 2019) and `month` (e.g.: 1, 2, ..., 12), based on `pickup_datetime`, to the core model to facilitate filtering for your queries

Now...
1. Create a new model `fct_fhv_monthly_zone_traveltime_p90.sql`
2. For each record in `dim_fhv_trips.sql`, compute the [timestamp_diff](https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#timestamp_diff) in seconds between dropoff_datetime and pickup_datetime - we'll call it `trip_duration` for this exercise
3. Compute the **continous** `p90` of `trip_duration` partitioning by year, month, pickup_location_id, and dropoff_location_id

For the Trips that **respectively** started from `Newark Airport`, `SoHo`, and `Yorkville East`, in November 2019, what are **dropoff_zones** with the 2nd longest p90 trip_duration ?

- LaGuardia Airport, Chinatown, Garment District
- LaGuardia Airport, Park Slope, Clinton East
- LaGuardia Airport, Saint Albans, Howard Beach
- LaGuardia Airport, Rosedale, Bath Beach
- LaGuardia Airport, Yorkville East, Greenpoint


**Staging model**

```sql

{{
    config(
        materialized='view'
    )
}}

with tripdata as 
(
  select *
  from {{ source('staging','FHV_2019') }}
  where dispatching_base_num is not null 
)
select

    dispatching_base_num,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropOff_datetime,
    PUlocationID,
    DOlocationID,
    SR_Flag,
    Affiliated_base_number
    

from tripdata

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}

```

**Core model**

```sql

{{
    config(
        materialized='table'
    )
}}

with tripdata as (
    select * from {{ ref('stg_fhv_tripdata') }}
), 

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select 

    tripdata.dispatching_base_num, 
    tripdata.pickup_datetime, 
    tripdata.dropOff_datetime,
    EXTRACT(YEAR FROM tripdata.pickup_datetime) AS year,
    EXTRACT(MONTH FROM tripdata.pickup_datetime) AS month,
    tripdata.PUlocationID, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    tripdata.DOlocationID, 
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    tripdata.SR_Flag,
    tripdata.Affiliated_base_number

from tripdata
inner join dim_zones as pickup_zone
on tripdata.PUlocationID = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on tripdata.DOlocationID = dropoff_zone.locationid

```

**fct_fhv_monthly_zone_traveltime_p90.sql**


```sql

{{
    config(
        materialized='table'
    )
}}

with trip_duration_calculated as (
    select
        *,
        timestamp_diff(dropOff_datetime, pickup_datetime, second) as trip_duration
    from {{ ref('dim_fhv_trips') }}
)

select 

    *,
    PERCENTILE_CONT(trip_duration, 0.90) 
    OVER (PARTITION BY year, month, PUlocationID, DOlocationID) AS trip_duration_p90


from trip_duration_calculated

```

**dbt build**

run:

```
dbt build --select +fct_fhv_monthly_zone_traveltime_p90.sql+ --vars '{is_test_run: false}'
```


**Query**


```sql

WITH ranked_data AS (
    SELECT 
        pickup_zone,
        dropoff_zone,
        trip_duration_p90,
        DENSE_RANK() OVER (PARTITION BY pickup_zone ORDER BY trip_duration_p90 DESC) AS rank

    FROM `eastern-amp-449614-e1.dbt_aahirkar.fct_fhv_monthly_zone_traveltime_p90`
    WHERE month = 11 AND year = 2019 AND pickup_zone IN ('Newark Airport', 'SoHo', 'Yorkville East')
)

SELECT DISTINCT 
    pickup_zone, 
    dropoff_zone, 
    trip_duration_p90
    
FROM ranked_data
WHERE rank = 2;

```

**Results**

<br>

![ae65](images/ae65.jpg)
<br>