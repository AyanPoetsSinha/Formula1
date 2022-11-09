# Databricks notebook source
# MAGIC %md
# MAGIC ###Create dataframe for all the df

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/ayanstorage0001/processed

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

constructors_df=spark.read.parquet(f"{processed_folder_path}/constructors") \
.withColumnRenamed("name","team")

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

drivers_df=spark.read.parquet(f"{processed_folder_path}/drivers") \
.withColumnRenamed("number","driver_number") \
.withColumnRenamed("name","driver_name") \
.withColumnRenamed("nationality","driver_nationality")

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

circuits_df=spark.read.parquet(f"{processed_folder_path}/circuits") \
.withColumnRenamed("location","circuit_location") \
.withColumnRenamed("circuit_Id","circuit_id") \
.withColumnRenamed("name","circuit_name") \
.withColumnRenamed("Latitufe","Latitude")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

results_df=spark.read.parquet(f"{processed_folder_path}/results") \
.withColumnRenamed("time","race_time") \
.withColumnRenamed("number","race_number") \
.withColumnRenamed("grid","race_grid") \
.withColumnRenamed("position","race_position") \
.withColumnRenamed("points","race_points") \
.withColumnRenamed("laps","race_laps") \
.withColumnRenamed("rank","race_rank") \
.withColumnRenamed("races_timestamp","race_date")


# COMMAND ----------

display(results_df)

# COMMAND ----------

races_df=spark.read.parquet(f"{processed_folder_path}/races") \
.withColumnRenamed("name","race_name") \
.withColumnRenamed("round","race_round") \
.withColumnRenamed("races_timestamp","race_date")

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC 
# MAGIC %md
# MAGIC ###joining circuits to races dataframe

# COMMAND ----------

race_circuits_df=races_df.join(circuits_df, races_df.circuit_id==circuits_df.circuit_id,"inner") \
.select(races_df.race_id,races_df.race_year,races_df.race_name,races_df.race_date,circuits_df.circuit_location)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###join results with all other dataframe

# COMMAND ----------

race_results_df= results_df.join(race_circuits_df,race_circuits_df.race_id==races_df.race_id) \
    .join(drivers_df, results_df.driver_id==drivers_df.driver_id) \
    .join(constructors_df, constructors_df.constructor_id==results_df.constructor_id)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df=race_results_df.select("race_year","race_name","race_name","circuit_location","driver_name","driver_number","driver_nationality","team","race_grid","fastest_lap","race_time","race_points") \
.withColumn("created_date",current_timestamp())

# COMMAND ----------

display(final_df)

# COMMAND ----------

display(final_df.filter("race_year== 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(final_df.race_points.desc()))

# COMMAND ----------

  
