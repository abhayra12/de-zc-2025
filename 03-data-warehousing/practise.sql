-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `eastern-amp-449614-e1.de_03_DW.external_yellow_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://abhay-ra-kestra-bucket/yellow_tripdata_2019-02.csv']
);

-- Check yellow trip data 
SELECT  * FROM eastern-amp-449614-e1.de_03_DW.external_yellow_tripdata limit 10;

-- Creating a non partitioned table from external table
CREATE OR REPLACE TABLE `eastern-amp-449614-e1.de_03_DW.yellow_tripdata_non_partitioned` AS 
SELECT * FROM `eastern-amp-449614-e1.de_03_DW.external_yellow_tripdata` ;

-- Creating a partitioned table from external table 
CREATE OR REPLACE TABLE eastern-amp-449614-e1.de_03_DW.yellow_tripdata_partitioned
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT * FROM eastern-amp-449614-e1.de_03_DW.external_yellow_tripdata;

-- Scanning non partitioned data (process 107 MB)
SELECT DISTINCT(VendorID)
FROM `eastern-amp-449614-e1.de_03_DW.yellow_tripdata_non_partitioned`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-01-01' AND '2019-01-30';

-- Scanning partitioned data (process 10 MB)
SELECT DISTINCT(VendorID)
FROM `eastern-amp-449614-e1.de_03_DW.yellow_tripdata_partitioned`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-01-01' AND '2019-01-30'; 

-- Look into the partitions 
SELECT table_name, partition_id, total_rows
FROM `de_03_DW.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'yellow_tripdata_partitioned'
ORDER BY total_rows DESC;

-- Creating partitoned and clustered table 
CREATE OR REPLACE TABLE eastern-amp-449614-e1.de_03_DW.yellow_tripdata_partitioned_clustered
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM eastern-amp-449614-e1.de_03_DW.external_yellow_tripdata;

-- Query scans 1.1 GB
SELECT count(*) as trips
FROM eastern-amp-449614-e1.de_03_DW.yellow_tripdata_partitioned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID=1;

-- Query scans 864.5 MB
SELECT count(*) as trips
FROM eastern-amp-449614-e1.de_03_DW.yellow_tripdata_partitioned_clustered
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID=1;

