# Databricks notebook source
# MAGIC %fs
# MAGIC 
# MAGIC ls /mnt/ayanstorage0001/processed/results/

# COMMAND ----------

df=spark.read.parquet("/mnt/ayanstorage0001/processed/bbc_raceresult")

# COMMAND ----------

display(df)

# COMMAND ----------

demo_df=df.filter("race_year='2021'")

# COMMAND ----------

display(demo_df
       )

# COMMAND ----------

from pyspark.sql.functions import count,countDistinct,sum

# COMMAND ----------

demo_df.select(count("*")).show()

# COMMAND ----------

demo_df.select(count("race_name")).show()

# COMMAND ----------

demo_df.select(countDistinct("race_name")).show()

# COMMAND ----------

demo_df.select(sum("points")).show()

# COMMAND ----------

demo_df.filter("driver_name= 'Lewis Hamilton'").select(sum("points"), countDistinct("race_name")) \
.withColumnRenamed("sum(points)","total points") \
.withColumnRenamed("count(DISTINCT race_name)","Total Races") \
.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Group Aggregations

# COMMAND ----------

demo_df \
.groupBy("driver_name") \
.sum("points").show()

# COMMAND ----------

demo_df \
.groupBy("driver_name") \
.agg(sum("points"),countDistinct ("race_name")) \
.show()

# COMMAND ----------

demo_df \
.groupBy("driver_name") \
.agg(sum("points"),countDistinct ("race_name")) \
.orderBy("sum(points)", ascending=False) \
.show()

# COMMAND ----------

demo_df \
.groupBy("driver_name") \
.agg(sum("points").alias("Total Points"),countDistinct ("race_name").alias("Total Races")) \
.orderBy("Total Points", ascending=False) \
.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### window funcitons

# COMMAND ----------

demo_df=df.filter("race_year in ('2020','2021')")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

demo_df \
.groupBy("race_year","driver_name") \
.agg(sum("points").alias("Total Points"),countDistinct ("race_name").alias("Total Races")) \
.orderBy("Total Points", ascending=False) \
.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ranking

# COMMAND ----------

# MAGIC %md
# MAGIC ### importing

# COMMAND ----------

from pyspark.sql.window import WIndow
from pyspark.sql.functions import desc

# COMMAND ----------

# MAGIC %md
# MAGIC ###
# MAGIC creating specificaiton

# COMMAND ----------

driverRankSpec=Window.partitionBy("race_year").orderBy(desc(total_points))

# COMMAND ----------

demo_df.withColumn("rank", rank().over(driverRankSpec)).show()

# COMMAND ----------


