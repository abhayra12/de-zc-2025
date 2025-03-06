# Module 5 Homework

## Question 1: Install Spark and PySpark

1. Run PySpark
2. Create a local spark session
3. Execute spark.version.

What's the output?

```
'3.3.2
```

## Question 2: Yellow October 2024

Read the October 2024 Yellow into a Spark Dataframe. Repartition the Dataframe to 4 partitions and save it to parquet.

What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

```python

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName('test').getOrCreate()

df = spark.read.parquet('yellow_tripdata_2024-10.parquet')
df = df.repartition(4)
df.write.parquet('hw/2')

```

Result:

<br>

![q2](images/q2.jpg)

<br>

## Question 3: Count records

How many taxi trips were there on the 15th of October?

Consider only trips that started on the 15th of October.


```sql

df.registerTempTable('trips_data')  

spark.sql("""

SELECT count(1) FROM trips_data 
WHERE DATE(tpep_pickup_datetime) = '2024-10-15';
""").show()

```

Result:

```
128893
```

## Question 4: Longest trip

What is the length of the longest trip in the dataset in hours?

```python

spark.sql("""
SELECT 
    (unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 3600 AS trip_duration
FROM trips_data
ORDER BY trip_duration DESC
LIMIT 5;
""").show()                  

```

<br>

![q4](images/q4.jpg)

<br>

## Question 5: User Interface

Spark’s User Interface which shows the application's dashboard runs on which local port?

4040


## Question 6: Least frequent pickup location zone

Using the zone lookup data and the Yellow October 2024 data, what is the name of the LEAST frequent pickup location Zone?


```python

lookup_df = spark.read.option("header", "true").csv('taxi_zone_lookup.csv')

lookup_df.registerTempTable('lookup')  

spark.sql("""

SELECT lookup.Zone , count(1) as num_trips FROM trips_data 
INNER JOIN lookup ON lookup.LocationID = trips_data.PULocationID
GROUP BY lookup.Zone
ORDER BY num_trips ASC;
""").show()  

```

Result:

<br>

![q6](images/q6.jpg)

<br>