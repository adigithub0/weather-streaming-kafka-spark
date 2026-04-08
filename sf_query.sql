-- Database
CREATE DATABASE IF NOT EXISTS WEATHER_DB;
USE DATABASE WEATHER_DB;

-- Schemas
CREATE SCHEMA IF NOT EXISTS BRONZE;
CREATE SCHEMA IF NOT EXISTS SILVER;
CREATE SCHEMA IF NOT EXISTS GOLD;

-- Bronze Table (Raw JSON)
CREATE OR REPLACE TABLE BRONZE.RAW_WEATHER (
    raw_content VARIANT
);

-- Silver Table (Structured)
CREATE OR REPLACE TABLE SILVER.CLEAN_WEATHER AS
SELECT
    raw_content:city::STRING AS city,
    raw_content:country::STRING AS country,
    raw_content:temperature::FLOAT AS temperature,
    raw_content:humidity::FLOAT AS humidity,
    raw_content:timestamp::TIMESTAMP AS timestamp
FROM BRONZE.RAW_WEATHER;

-- Gold Table (Aggregated)
CREATE OR REPLACE TABLE GOLD.WEATHER_AGG AS
SELECT
    country,
    DATE(timestamp) AS date,
    AVG(temperature) AS avg_temp,
    AVG(humidity) AS avg_humidity
FROM SILVER.CLEAN_WEATHER
GROUP BY country, DATE(timestamp);

use database weather_db;
-- Stream (detect new data)
CREATE OR REPLACE STREAM raw_stream
ON TABLE BRONZE.RAW_WEATHER;

-- Task (auto transform Bronze → Silver)
CREATE OR REPLACE TASK bronze_to_silver
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 MINUTE'
AS
INSERT INTO SILVER.CLEAN_WEATHER
SELECT
    raw_content:city::STRING,
    raw_content:country::STRING,
    raw_content:temperature::FLOAT,
    raw_content:humidity::FLOAT,
    raw_content:timestamp::TIMESTAMP
FROM raw_stream;

-- Enable task
ALTER TASK bronze_to_silver RESUME;

use database weather_db;

SELECT * FROM BRONZE.RAW_WEATHER;
SELECT * FROM SILVER.CLEAN_WEATHER;
SELECT * FROM GOLD.WEATHER_AGG;

SHOW TABLES IN DATABASE WEATHER_DB;
SHOW VIEWS IN DATABASE WEATHER_DB;