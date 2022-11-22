# Databricks notebook source
# MAGIC %md
# MAGIC ### Produce Driver Standing

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df= spark.read.parquet(f"{presentation_folder_path}/raceresults")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum, when,count,col,desc,asc

# COMMAND ----------

from pyspark.sql.functions import sum, when,count,col,desc,asc

driver_standing_df=race_results_df \
.groupBy("race_year", "driver_name", "driver_nationality", "team") \
.agg(sum("points").alias("total_points"),
    count(when(col("position")==1,True)).alias("Wins"))

# COMMAND ----------

display(driver_standing_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, desc, asc

driver_rank_spec=Window.partitionBy("race_year").orderBy(desc("total_points"),asc("Wins"))
final_df = driver_standing_df.withColumn("rank",rank().over(driver_rank_spec))


# COMMAND ----------

display(final_df)

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driverstandings")

# COMMAND ----------


