# Databricks notebook source
# MAGIC %md
# MAGIC ###Create dataframe for all the df

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/ayanstorage0001/processed

# COMMAND ----------

constructors_df=spark.read.parquet(f"{processed_folder_path}/constructors") \
.withColumnRenamed("name","team")

# COMMAND ----------

drivers_df=spark.read.parquet(f"{processed_folder_path}/drivers") \
.withColumnRenamed("number","drivers_number") \
.withColumnRenamed("name","driver_name") \
.withColumnRenamed("nationality","driver_nationality")

# COMMAND ----------

circuits_df=spark.read.parquet(f"{processed_folder_path}/circuits") \
.withColumnRenamed("location","circuit_location") 

# COMMAND ----------

sprint_results_df=spark.read.parquet(f"{processed_folder_path}/sprint_results") \
.withColumnRenamed("") \
.withColumnRenamed("number","drivers_number") \
.withColumnRenamed("name","driver_name") \
.withColumnRenamed("nationality" "driver+nationality")

# COMMAND ----------

races_df=spark.read.parquet(f"{processed_folder_path}/races") \
.withColumnRenamed("name","race_name") \
.withColumnRenamed("race_timestamp","race_date")

# COMMAND ----------




# COMMAND ----------


