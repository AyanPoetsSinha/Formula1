-- Databricks notebook source
create database if not exists  f1_raw;


-- COMMAND ----------

use f1_raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Create circuit tables

-- COMMAND ----------

drop table if exists f1_raw.circuits;
create table if not exists f1_raw.circuits(circuitId INT,
circuitRef STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING)
using csv
options (path "/mnt/ayanstorage0001/raw/circuits.csv", header true)

-- COMMAND ----------

show tables;

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls  "/mnt/ayanstorage0001/raw"

-- COMMAND ----------

select * from f1_raw.circuits

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Create races tables

-- COMMAND ----------

drop table if exists f1_raw.races;
create table if not exists f1_raw.races(raceId INT,
year INT,
round INT,
circuitId INT,
name STRING,
date DATE,
time STRING,
url STRING
)
using csv
options (path "/mnt/ayanstorage0001/raw/races.csv", header true)

-- COMMAND ----------

select * from f1_raw.races;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create table for JSON Files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### create consturctors table
-- MAGIC - single line JSON
-- MAGIC - Simple Structure

-- COMMAND ----------

drop table if exists f1_raw.constructors;
create table if not exists f1_raw.constructors(constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING
)
using json
options (path "/mnt/ayanstorage0001/raw/constructors.json", header true)

-- COMMAND ----------

select * from f1_raw.constructors;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### create drivers table
-- MAGIC - single line JSON
-- MAGIC - Complex Structure

-- COMMAND ----------

drop table if exists f1_raw.drivers;
create table if not exists f1_raw.drivers(driverId INT,
driverRef INT,
number INT,
code STRING,
name STRUCT<forename: STRING, surname: STRING>,
dob DATE,
nationality STRING,
url STRING
)
using json
options (path "/mnt/ayanstorage0001/raw/drivers.json", header true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### create results table
-- MAGIC - single line JSON
-- MAGIC - simple Structure

-- COMMAND ----------

drop table if exists f1_raw.results;
create table if not exists f1_raw.drivers(driverId INT,
driverRef INT,
number INT,
code STRING,
name STRUCT<forename: STRING, surname: STRING>,
dob DATE,
nationality STRING,
url STRING
)
using json
options (path "/mnt/ayanstorage0001/raw/drivers.json", header true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### create pitstops table
-- MAGIC - multi line JSON
-- MAGIC - complex Structure

-- COMMAND ----------


