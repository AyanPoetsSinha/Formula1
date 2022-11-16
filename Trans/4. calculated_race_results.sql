-- Databricks notebook source
use f1_processed;

-- COMMAND ----------

drop table if exists  f1_presentation.calculated_race_results;
create table f1_presentation.calculated_race_results
using PARQUET
as
select races.race_year,
        constructors.name AS Constructor,
        drivers.name as Driver,
        results.position,
        results.points,
        11-results.position as calculated_points
from results
join f1_processed.drivers
ON results.driver_id = drivers.driver_id
join f1_processed.constructors
ON results.constructor_id= constructors.constructor_id
join f1_processed.races 
on results.race_id=races.race_id
where results.position <=10;

-- COMMAND ----------

select * from f1_presentation.calculated_race_results

-- COMMAND ----------


