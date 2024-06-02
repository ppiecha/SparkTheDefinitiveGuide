-- Databricks notebook source
-- MAGIC %scala
-- MAGIC spark.sql("""
-- MAGIC   CREATE TABLE flights (
-- MAGIC   DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count LONG)
-- MAGIC   USING JSON OPTIONS (path '/databricks-datasets/definitive-guide/data/flight-data/json/2015-summary.json')
-- MAGIC """)

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC spark.sql("""
-- MAGIC   CREATE TABLE flights_csv (
-- MAGIC   DEST_COUNTRY_NAME STRING,
-- MAGIC   ORIGIN_COUNTRY_NAME STRING COMMENT "remember, the US will be most prevalent",
-- MAGIC   count LONG)
-- MAGIC   USING csv OPTIONS (header true, path '/databricks-datasets/definitive-guide/data/flight-data/csv/2015-summary.csv')
-- MAGIC """)  

-- COMMAND ----------

CREATE TABLE flights_from_select USING parquet AS SELECT * FROM flights

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS flights_from_select
AS SELECT * FROM flights

-- COMMAND ----------

CREATE TABLE partitioned_flights USING parquet PARTITIONED BY (DEST_COUNTRY_NAME)
AS SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME, count FROM flights LIMIT 5

-- COMMAND ----------

DESCRIBE TABLE flights_csv

-- COMMAND ----------

SHOW PARTITIONS partitioned_flights

-- COMMAND ----------

CACHE TABLE flights

-- COMMAND ----------

UNCACHE TABLE FLIGHTS

-- COMMAND ----------

CREATE VIEW just_usa_view AS
SELECT * FROM flights WHERE dest_country_name = 'United States'

-- COMMAND ----------

CREATE TEMP VIEW just_usa_view_temp AS
SELECT * FROM flights WHERE dest_country_name = 'United States'

-- COMMAND ----------

CREATE GLOBAL TEMP VIEW just_usa_global_view_temp AS
SELECT * FROM flights WHERE dest_country_name = 'United States'

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW just_usa_view_temp AS
SELECT * FROM flights WHERE dest_country_name = 'United States'

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

CREATE DATABASE some_db

-- COMMAND ----------

USE some_db

-- COMMAND ----------

SHOW tables

-- COMMAND ----------

SELECT * FROM flights -- fails with table/view not found

-- COMMAND ----------

SELECT * FROM default.flights

-- COMMAND ----------

SELECT current_database()
