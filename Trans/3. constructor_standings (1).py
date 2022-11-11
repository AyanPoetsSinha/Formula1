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

from pyspark.sql.functions import sum, when,count,col,desc,asc

# COMMAND ----------

from pyspark.sql.functions import sum, when,count,col,desc,asc

constructor_standing_df=race_results_df \
.groupBy("race_year", "team") \
.agg(sum("points").alias("total_points"),
    count(when(col("position")==1,True)).alias("Wins"))

# COMMAND ----------

display(constructor_standing_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, desc, asc

constructor_rank_spec=Window.partitionBy("race_year").orderBy(desc("total_points"),asc("Wins"))
final_df = constructor_standing_df.withColumn("rank",rank().over(constructor_rank_spec))


# COMMAND ----------

display(final_df)

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/constructor_standings")

# COMMAND ----------


