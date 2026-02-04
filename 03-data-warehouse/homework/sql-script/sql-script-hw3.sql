-- CREATE EXTERNAL TABLE FROM GCP BUCKET --
CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-project-485521.datawarehouse_nyc_zoomcamp.yellow_tripdata_2024_ext`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dario-datawarehouse-de-zoomcamp/yellow-taxi-jan-to-jun-2024/yellow_tripdata_2024-*.parquet']
);

-- CREATE REGULAR TABLE FROM EXTENAL TABLE -- 
CREATE OR REPLACE TABLE `de-zoomcamp-project-485521.datawarehouse_nyc_zoomcamp.yellow_tripdata_2024`
AS 
SELECT * 
FROM `de-zoomcamp-project-485521.datawarehouse_nyc_zoomcamp.yellow_tripdata_2024_ext`;

-- COUNT DISTINCT PULOCATIONID USING EXTERNAL TABLE -- 

SELECT COUNT (DISTINCT(PULocationID))
FROM  `de-zoomcamp-project-485521.datawarehouse_nyc_zoomcamp.yellow_tripdata_2024_ext` ;

-- COUNT DISTINCT PULOCATIONID USING INTERNAL TABLE -- 
SELECT COUNT (DISTINCT(PULocationID))
FROM  `de-zoomcamp-project-485521.datawarehouse_nyc_zoomcamp.yellow_tripdata_2024` ;

-- QUERY TO RETRIEVE PULOCATIONID FROM REGULAR TABLE -- 
SELECT PULOCATIONID 
FROM `de-zoomcamp-project-485521.datawarehouse_nyc_zoomcamp.yellow_tripdata_2024`

-- How many records have a fare_amount of 0 --
SELECT COUNT(fare_amount) AS fare_amount_0
FROM `de-zoomcamp-project-485521.datawarehouse_nyc_zoomcamp.yellow_tripdata_2024_ext` 
WHERE fare_amount = 0;

-- QEUERY TO RETRIEVE PULOCATIONID & DOLOCATIONID FROM REGULAR TABLE --
SELECT PULOCATIONID, DOLocationID
FROM `de-zoomcamp-project-485521.datawarehouse_nyc_zoomcamp.yellow_tripdata_2024`

-- Partition the table by tpep_pickup_datetime and cluster by vendorid --
CREATE OR REPLACE TABLE `de-zoomcamp-project-485521.datawarehouse_nyc_zoomcamp.yellow_tripdata_2024_part_clust`
PARTITION BY 
  DATE(tpep_dropoff_datetime)
CLUSTER BY 
  VendorID AS 
SELECT * 
FROM  `de-zoomcamp-project-485521.datawarehouse_nyc_zoomcamp.yellow_tripdata_2024_ext`;

--  query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive) using non partitioned table --
SELECT VendorID
FROM  `de-zoomcamp-project-485521.datawarehouse_nyc_zoomcamp.yellow_tripdata_2024_ext`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

--  query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive) using partitioned table --
SELECT VendorID
FROM  `de-zoomcamp-project-485521.datawarehouse_nyc_zoomcamp.yellow_tripdata_2024_part_clust`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

-- Select all the values from the regular table --
SELECT * 
FROM `de-zoomcamp-project-485521.datawarehouse_nyc_zoomcamp.yellow_tripdata_2024`; 