# Databricks notebook source
# MAGIC %md
# MAGIC ### Produce Driver Standing

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df= spark.read.parquet(f"{presentation_folder_path}/race_result")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum, when

# COMMAND ----------

driver_standing_df=race_results_df \
.groupBy("race_year", "driver_name", "driver_nationality", "team") \
.agg(sum("points").alias("total_points"),
    count(when())

# COMMAND ----------


