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
options (path "/mnt/ayanstorage0001/raw/constructors.json")

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
options (path "/mnt/ayanstorage0001/raw/drivers.json")

-- COMMAND ----------

select * from f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### create results table
-- MAGIC - single line JSON
-- MAGIC - simple Structure

-- COMMAND ----------

drop table if exists f1_raw.results;
create table if not exists f1_raw.results(resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,
grid INT,
position INT,
positionText STRING,
positionOrder INT,
points INT,
laps INT,
time STRING,
miliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed FLOAT,
statusId STRING
)
using json
options (path "/mnt/ayanstorage0001/raw/results.json")

-- COMMAND ----------

select * from f1_raw.results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### create pitstops table
-- MAGIC - multi line JSON
-- MAGIC - complex Structure

-- COMMAND ----------

drop table if exists f1_raw.pitstops;
create table if not exists f1_raw.pitstops(
raceId INT,
driverId INT,
stop STRING,
lap INT,
time STRING,
duration STRING,
miliseconds INT
)
using json
options (path "/mnt/ayanstorage0001/raw/pit_stops.json", multiLine true)

-- COMMAND ----------

show tables;


-- COMMAND ----------

select * from f1_raw.pitstops

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##create tables for the list of files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##creating tbale for laptime
-- MAGIC 
-- MAGIC - csv files
-- MAGIC - multiple files

-- COMMAND ----------

drop table if exists f1_raw.laptimes;
create table if not exists f1_raw.laptimes(
raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
miliseconds INT
)
using csv
options (path "/mnt/ayanstorage0001/raw/lap_times")

-- COMMAND ----------

select * from f1_raw.laptimes;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##create qualifying table
-- MAGIC 
-- MAGIC - JSON file
-- MAGIC - multi line json
-- MAGIC - multiple files

-- COMMAND ----------

drop table if exists f1_raw.qualifying;
create table if not exists f1_raw.qualifying(
qualifyId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING
)
using json
options (path "/mnt/ayanstorage0001/raw/qualifying", multiLine true)

-- COMMAND ----------

select * from f1_raw.qualifying;
