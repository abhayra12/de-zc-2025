# DBT Project Setup Guide: NYC Taxi Rides

This guide will walk you through setting up and developing a DBT project for analyzing NYC taxi ride data using DBT Cloud.

## Table of Contents
- [1. Setting up Source Data](#1-setting-up-source-data)
- [2. Setting up DBT Project on DBT Cloud](#2-setting-up-dbt-project-on-dbt-cloud)
- [3. Adding Staging Models](#3-adding-staging-models)
- [4. Adding Seeds](#4-adding-seeds)
- [5. Adding Core Models](#5-adding-core-models)
- [6. Adding Data Marts](#6-adding-data-marts)
- [7. Automating Testing](#7-automating-testing)
- [8. Automatic Documentation Generation](#8-automatic-documentation-generation)
- [9. Creating Production Environment](#9-creating-production-environment)
- [10. Setting up CI/CD](#10-setting-up-ci-cd)

## 1. Setting up Source Data

### Prerequisites
- A BigQuery account with a project created
- Source data loaded into BigQuery

### Loading Data into BigQuery
1. Create a new dataset in your BigQuery project named `trips_data_all`

2. Import the following tables from the public dataset:
   ```sql
   -- Yellow taxi data
   CREATE OR REPLACE EXTERNAL TABLE `your-project-id.trips_data_all.yellow_tripdata`
   OPTIONS (
     format = 'PARQUET',
     uris = ['gs://nyc-tl-data/trip data/yellow_tripdata_*.parquet']
   );

   -- Green taxi data
   CREATE OR REPLACE EXTERNAL TABLE `your-project-id.trips_data_all.green_tripdata`
   OPTIONS (
     format = 'PARQUET',
     uris = ['gs://nyc-tl-data/trip data/green_tripdata_*.parquet']
   );

   -- For-Hire Vehicle (FHV) data
   CREATE OR REPLACE EXTERNAL TABLE `your-project-id.trips_data_all.fhv_tripdata`
   OPTIONS (
     format = 'PARQUET',
     uris = ['gs://nyc-tl-data/trip data/fhv_tripdata_*.parquet']
   );
   ```

## 2. Setting up DBT Project on DBT Cloud

### Create a DBT Cloud Account
1. Go to [dbt Cloud](https://cloud.getdbt.com/) and sign up for a new account
2. Choose BigQuery as your data warehouse
3. Complete the initial setup process

### Connect to BigQuery
1. In DBT Cloud settings, navigate to **Deploy** → **Environment**
2. Click **Add Credentials**
3. Upload your BigQuery service account JSON key file
4. Test the connection to ensure it works

### Initialize Project
1. Create a new project in DBT Cloud
2. Name it `taxi_rides_ny`
3. Configure the following settings:
   - Development credentials: Your BigQuery credentials
   - Project Settings:
     ```yaml
     name: 'taxi_rides_ny'
     config-version: 2
     version: '0.1'
     ```

## 3. Adding Staging Models

### Create Project Structure
1. In your project, create the following directory structure:
   ```
   models/
   ├── staging/
   │   ├── schema.yml
   │   ├── stg_green_tripdata.sql
   │   └── stg_yellow_tripdata.sql
   └── core/
   ```

### Configure Source Tables
Create `models/staging/schema.yml`:

```yaml
version: 2

sources:
    - name: staging
      database: your-project-id
      schema: trips_data_all

      tables:
          - name: green_tripdata
          - name: yellow_tripdata
          - name: fhv_tripdata
```

### Create Staging Models
Create `models/staging/stg_green_tripdata.sql`:

```sql
{{ config(materialized='view') }}

select
    -- identifiers
    {{ dbt_utils.surrogate_key(['vendorid', 'lpep_pickup_datetime']) }} as tripid,
    cast(vendorid as integer) as vendorid,
    cast(ratecodeid as integer) as ratecodeid,
    cast(pulocationid as integer) as  pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,
    
    -- timestamps
    cast(lpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    store_and_fwd_flag,
    cast(passenger_count as integer) as passenger_count,
    cast(trip_distance as numeric) as trip_distance,
    cast(trip_type as integer) as trip_type,
    
    -- payment info
    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,
    cast(ehail_fee as numeric) as ehail_fee,
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    cast(total_amount as numeric) as total_amount,
    cast(payment_type as integer) as payment_type,
    cast(congestion_surcharge as numeric) as congestion_surcharge
    
from {{ source('staging','green_tripdata') }}
where vendorid is not null
```

Create `models/staging/stg_yellow_tripdata.sql`:

```sql
{{ config(materialized='view') }}

select
    -- identifiers
    {{ dbt_utils.surrogate_key(['vendorid', 'tpep_pickup_datetime']) }} as tripid,
    cast(vendorid as integer) as vendorid,
    cast(ratecodeid as integer) as ratecodeid,
    cast(pulocationid as integer) as  pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,
    
    -- timestamps
    cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    store_and_fwd_flag,
    cast(passenger_count as integer) as passenger_count,
    cast(trip_distance as numeric) as trip_distance,
    
    -- payment info
    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    cast(total_amount as numeric) as total_amount,
    cast(payment_type as integer) as payment_type,
    cast(congestion_surcharge as numeric) as congestion_surcharge

from {{ source('staging','yellow_tripdata') }}
where vendorid is not null
```

### Project Structure
![image](https://user-images.githubusercontent.com/4315804/152691312-e71b56a4-53ff-4884-859c-c9090dbd0db8.png)

### Development Workflow
![image](https://user-images.githubusercontent.com/4315804/148699280-964c4e0b-e685-4c0f-a266-4f3e097156c9.png)

## 4. Adding Seeds

### Add Reference Data
1. Create a `seeds` directory in your project root
2. Add `taxi_zone_lookup.csv` with the following structure:
   ```csv
   locationid,borough,zone,service_zone
   1,EWR,Newark Airport,EWR
   2,Queens,Jamaica Bay,Boro Zone
   3,Bronx,Allerton/Pelham Gardens,Boro Zone
   ...
   ```

### Configure Seeds
Add the following to your `dbt_project.yml`:

```yaml
seeds:
    taxi_rides_ny:
        taxi_zone_lookup:
            +column_types:
                locationid: numeric
```

### Load Seeds
Run the following command in the dbt Cloud IDE:
```bash
dbt seed
```

This will load the taxi zone lookup data into your data warehouse.

## 5. Adding Core Models

### Create Core Models
Create `models/core/fact_trips.sql`:
```sql
{{ config(materialized='table') }}

with green_data as (
    select *, 
        'Green' as service_type 
    from {{ ref('stg_green_tripdata') }}
), 

yellow_data as (
    select *, 
        'Yellow' as service_type
    from {{ ref('stg_yellow_tripdata') }}
), 

trips_unioned as (
    select * from green_data
    union all
    select * from yellow_data
),

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select 
    trips_unioned.tripid, 
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
    trips_unioned.improvement_surcharge, 
    trips_unioned.total_amount, 
    trips_unioned.payment_type, 
    trips_unioned.congestion_surcharge
from trips_unioned
inner join dim_zones as pickup_zone
on trips_unioned.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on trips_unioned.dropoff_locationid = dropoff_zone.locationid
```

Create `models/core/dim_zones.sql`:
```sql
{{ config(materialized='table') }}

select 
    locationid, 
    borough, 
    zone, 
    replace(service_zone,'Boro','Green') as service_zone
from {{ ref('taxi_zone_lookup') }}
```

## 6. Adding Data Marts

Create `models/core/dm_monthly_zone_revenue.sql`:
```sql
{{ config(materialized='table') }}

with trips_data as (
    select 
        pickup_zone as revenue_zone,
        date_trunc(pickup_datetime, month) as revenue_month, 
        service_type, 
        sum(fare_amount) as revenue_monthly_fare,
        sum(tip_amount) as revenue_monthly_tips,
        sum(total_amount) as revenue_monthly_total
    from {{ ref('fact_trips') }}
    group by 1,2,3
)

select *
from trips_data
```

## 7. Automating Testing

Add to `models/staging/schema.yml`:
```yaml
models:
    - name: stg_green_tripdata
      description: Green taxi trips
      columns:
          - name: tripid
            description: Primary key for this table, generated with a concatenation of vendorid+pickup_datetime
            tests:
                - unique
                - not_null
          - name: fare_amount 
            description: The fare amount charged
            tests:
                - not_null
                - positive_values

    - name: stg_yellow_tripdata
      description: Yellow taxi trips
      columns:
          - name: tripid
            description: Primary key for this table, generated with a concatenation of vendorid+pickup_datetime
            tests:
                - unique
                - not_null
          - name: fare_amount 
            description: The fare amount charged
            tests:
                - not_null
                - positive_values
```

Create `tests/positive_values.sql`:
```sql
{% test positive_values(model, column_name) %}
select
    *
from {{ model }}
where {{ column_name }} < 0
{% endtest %}
```

## 8. Automatic Documentation Generation

Update `dbt_project.yml`:
```yaml
models:
  taxi_rides_ny:
    +materialized: view
    staging:
      +materialized: view
    core:
      +materialized: table
      
vars:
  payment_type_values: [1, 2, 3, 4, 5, 6]

docs-paths: ["docs"]
```

Create `models/staging/staging.md`:
```markdown
{% docs payment_type %}
	
Payment types:
* 1: Credit card
* 2: Cash
* 3: No charge
* 4: Dispute
* 5: Unknown
* 6: Voided trip

{% enddocs %}
```

Generate documentation:
```bash
dbt docs generate
dbt docs serve
```

## 9. Creating Production Environment

1. In dbt Cloud, go to **Deploy** → **Environments**
2. Click **New Environment**
3. Configure production environment:
   ```yaml
   Name: Production
   Type: Deployment
   dbt Version: 1.0.0
   Dataset: production
   ```

4. Create deployment credentials
5. Configure deployment schedule

## 10. Setting up CI/CD

1. In dbt Cloud, go to **Deploy** → **Jobs**
2. Create new job:
   ```yaml
   Name: dbt_production_run
   Environment: Production
   Commands:
     - dbt deps
     - dbt seed
     - dbt run
     - dbt test
     - dbt docs generate
   Schedule: Daily at 6AM
   ```

3. Configure GitHub integration:
   - Go to **Settings** → **Repository**
   - Connect your GitHub repository
   - Set up branch deployments

4. Create `.github/workflows/dbt.yml`:
```yaml
name: DBT CI

on:
  pull_request:
    branches: [ main ]

jobs:
  dbt:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - uses: actions/setup-python@v2
      - name: Install dependencies
        run: pip install dbt-bigquery
      - name: Run dbt
        env:
          DBT_PROFILES_DIR: ./ 
          BIGQUERY_TOKEN: ${{ secrets.BIGQUERY_TOKEN }}
        run: |
          dbt deps
          dbt compile
          dbt test
```

To run the entire pipeline:
```bash
dbt build --full-refresh
```

This will execute all models, tests, and documentation generation in the correct order.

For incremental updates:
```bash
dbt run
dbt test
```

Remember to commit all changes to version control and follow good Git practices:
```bash
git add .
git commit -m "Add models, tests, and documentation"
git push origin main
```

## Special Mentions

### Key Contributors
- Original dbt starter project team at dbt Labs
- NYC Taxi & Limousine Commission for providing the source data
- [DataTalksClub](https://datatalks.club/) community for support and feedback
- [Victoria Perez Mola](https://www.linkedin.com/in/victoriaperezmola/) for the comprehensive dbt course materials

### Additional Resources
- [dbt Developer Hub](https://docs.getdbt.com/docs/developer-resources)
- [NYC TLC Trip Record Data](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [BigQuery Documentation](https://cloud.google.com/bigquery/docs)

### License
This project guide is released under the MIT License. Feel free to use, modify and share while providing appropriate attribution.

