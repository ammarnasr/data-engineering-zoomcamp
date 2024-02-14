-- Creating external table referring to gcs paths after using mage 
CREATE OR REPLACE EXTERNAL TABLE `ny_taxi.external_green_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://ammar-demo-bucket-1/green_tripdata_2022-*.parquet']
);


-- Create a  table from external table
CREATE OR REPLACE TABLE ny_taxi.green_tripdata AS
SELECT * FROM ny_taxi.external_green_tripdata;


--Count distinct PULocationID from External Green Taxi Table
SELECT COUNT(DISTINCT PULocationID)  FROM ny_taxi.external_green_tripdata;

--Count distinct PULocationID from Green Taxi Table
SELECT COUNT(DISTINCT PULocationID)  FROM ny_taxi.green_tripdata;

--Count 0 Fare amount
SELECT COUNT(0) FROM ny_taxi.green_tripdata WHERE fare_amount=0;

-- Create a partitioned table from external table
CREATE OR REPLACE TABLE ny_taxi.green_tripdata_partitoned
PARTITION BY
  DATE(lpep_pickup_datetime) AS
SELECT * FROM ny_taxi.external_green_tripdata;





