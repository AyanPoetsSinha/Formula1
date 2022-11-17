-- Databricks notebook source
-- MAGIC %python
-- MAGIC html=""""<h1 style="text-align:center;font-family:Georgia;color:#000000;">F1 Dominant Team Dashboard</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

create or replace TEMP VIEW viz_dom_constructor
as
select constructor,sum(calculated_points)  as total_points
,count(1) as total_races
,avg(calculated_points) as avg_points,
RANK() OVER(ORDER BY AVG(calculated_points) DESC) constructor_rank
from f1_presentation.calculated_race_results
group by constructor
having count(1)>=100
order by avg_points desc

-- COMMAND ----------

select * from viz_dom_constructor

-- COMMAND ----------

select race_year,constructor,
sum(calculated_points)  as total_points
,count(1) as total_races
,avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where constructor in (select constructor from viz_dom_constructor where constructor_rank<=5)
group by constructor,race_year
order by avg_points desc

-- COMMAND ----------

select race_year,constructor,
sum(calculated_points)  as total_points
,count(1) as total_races
,avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where constructor in (select constructor from viz_dom_constructor where constructor_rank<=5)
group by constructor,race_year
order by race_year,avg_points desc

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


