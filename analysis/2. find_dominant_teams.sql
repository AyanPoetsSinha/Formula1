-- Databricks notebook source
select *
from f1_presentation.calculated_race_results

-- COMMAND ----------

select constructor, sum(calculated_points) as total_points,
count(1) as total_races,
avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where race_year between 2011 and 2022
group by constructor
order by avg_points desc

-- COMMAND ----------

select constructor, sum(calculated_points) as total_points,
count(1) as total_races,
avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where race_year between 2001 and 2010
group by constructor
order by avg_points desc

-- COMMAND ----------


