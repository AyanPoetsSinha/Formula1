-- Databricks notebook source
create or replace TEMP VIEW viz_dom_drivers 
as
select driver,sum(calculated_points)  as total_points
,count(1) as total_races
,avg(calculated_points) as avg_points,
RANK() OVER(ORDER BY AVG(calculated_points) DESC) driver_rank
from f1_presentation.calculated_race_results
group by driver
having total_races >=50
order by avg_points desc

-- COMMAND ----------

select race_year,driver,
sum(calculated_points)  as total_points
,count(1) as total_races
,avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where driver in (select driver from viz_dom_drivers where driver_rank<=10)
group by driver,race_year
order by avg_points desc

-- COMMAND ----------

select race_year,driver,
sum(calculated_points)  as total_points
,count(1) as total_races
,avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where driver in (select driver from viz_dom_drivers where driver_rank<=10)
group by driver,race_year
order by avg_points desc

-- COMMAND ----------

select race_year,driver,
sum(calculated_points)  as total_points
,count(1) as total_races
,avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where driver in (select driver from viz_dom_drivers where driver_rank<=10)
group by driver,race_year
order by avg_points desc

-- COMMAND ----------


