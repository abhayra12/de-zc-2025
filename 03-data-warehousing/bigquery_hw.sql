-- Create external table 
CREATE OR REPLACE EXTERNAL TABLE `eastern-amp-449614-e1.de_03_DW.external_yellow_tripdata`
OPTIONS (
    format = 'parquet',
    uris = ['gs://yellow_taxi_gcs/yellow_tripdata_2024-*.parquet']
);

-- Create native table 
CREATE OR REPLACE TABLE `eastern-amp-449614-e1.de_03_DW.yellow_tripdata_materialized` AS
SELECT * FROM `eastern-amp-449614-e1.de_03_DW.external_yellow_tripdata`;

-- Question 1: What is count of records for the 2024 Yellow Taxi Data?
SELECT count(*) FROM `eastern-amp-449614-e1.de_03_DW.external_yellow_tripdata`;

-- Question 2: Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.

-- reads 0 MB
SELECT count(distinct PULocationID) FROM `eastern-amp-449614-e1.de_03_DW.external_yellow_tripdata`;

-- reads 155 MB
SELECT count(distinct PULocationID) FROM `eastern-amp-449614-e1.de_03_DW.yellow_tripdata_materialized`;



-- Question 3: Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?
-- reads 155 MB
SELECT PULocationID FROM `eastern-amp-449614-e1.de_03_DW.yellow_tripdata_materialized`;
-- reads 310 MB
SELECT PULocationID, DOLocationID FROM `eastern-amp-449614-e1.de_03_DW.yellow_tripdata_materialized`;
--(bcoz bigquery is columnar store and it reads only the columns that are required)

-- Question 4: How many records have a fare_amount of 0?
SELECT count(*) FROM `eastern-amp-449614-e1.de_03_DW.yellow_tripdata_materialized` WHERE fare_amount = 0;


-- Question 5: What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)

-- Answer: Partition by tpep_dropoff_datetime and Cluster on VendorID

CREATE OR REPLACE TABLE `eastern-amp-449614-e1.de_03_DW.yellow_tripdata_partitioned_clustered` 
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `eastern-amp-449614-e1.de_03_DW.external_yellow_tripdata`;

-- Question 6: Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)

--- Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values? </br>
-- using materialized table
SELECT distinct VendorID FROM `eastern-amp-449614-e1.de_03_DW.yellow_tripdata_materialized` 
WHERE  tpep_dropoff_datetime >= '2024-03-01' 
AND tpep_dropoff_datetime <= '2024-03-15';
-- using partitioned table
SELECT distinct VendorID FROM `eastern-amp-449614-e1.de_03_DW.yellow_tripdata_partitioned_clustered` 
WHERE  tpep_dropoff_datetime >= '2024-03-01' 
AND  tpep_dropoff_datetime <= '2024-03-15';

-- Question 7: Where is the data stored in the External Table you created?
-- Answer: GCP Bucket

-- Question 8: It is best practice in Big Query to always cluster your data:
-- Answer: False

-- (Bonus: Not worth points) Question 9: No Points: Write a `SELECT count(*)` query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

SELECT count(*) FROM `eastern-amp-449614-e1.de_03_DW.yellow_tripdata_materialized`;

--Estimated bytes processed: 0 bytes
--Reason: BigQuery maintains pre-computed statistics about tables, including the total row count. For a simple COUNT(*) query without any WHERE clause, it can return this value from metadata without scanning the actual table data.

