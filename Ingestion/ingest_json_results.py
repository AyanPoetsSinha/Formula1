# Databricks notebook source
dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/ayanstorage0001/raw

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create a schema

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,FloatType

# COMMAND ----------


results_schema=StructType(fields=[StructField("resultId",IntegerType(),False),
                                 StructField("raceId",IntegerType(),False), 
                                 StructField("driverId",IntegerType(),False),
                                 StructField("constructorId",IntegerType(),False),
                                 StructField("number",IntegerType(),False),
                                 StructField("grid",IntegerType(),False),
                                 StructField("position",IntegerType(),False),
                                 StructField("positionText",StringType(),False),
                                 StructField("positionOrder",IntegerType(),False),
                                 StructField("points",FloatType(),False),
                                 StructField("laps",IntegerType(),False),
                                 StructField("time",StringType(),False),
                                 StructField("miliseconds",IntegerType(),False),
                                 StructField("fastestLap",IntegerType(),False),
                                 StructField("rank",IntegerType(),False),
                                 StructField("fastestLapTime",StringType(),False),
                                 StructField("fastestLapSpeed",StringType(),False),
                                 StructField("statusId",IntegerType(),False),
                                
                                 ])

# COMMAND ----------

# MAGIC %md
# MAGIC ###attach the table with schema and read it

# COMMAND ----------

results_df=spark.read \
.schema(results_schema) \
.json('/mnt/ayanstorage0001/raw/results.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Change the attribute name, add new column with time and drop the unnecessary columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

results_changed_df=results_df \
.withColumnRenamed("resultId","result_id") \
.withColumnRenamed("raceId","race_id") \
.withColumnRenamed("driverId","driver_id") \
.withColumnRenamed("constructorId","constructor_id") \
.withColumnRenamed("positionText","postion_text") \
.withColumnRenamed("positionOrder","position_order") \
.withColumnRenamed("fastestLap","fastest_lap") \
.withColumnRenamed("fastestLapTime","fastest_lap_time") \
.withColumnRenamed("fastestLapSpeed","fastest_lap_speed") \
.withColumn("ingestion_date",current_timestamp())


# COMMAND ----------

display(results_changed_df)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

results_final_df= results_changed_df. \
select(col("result_id"),
      col("race_id"),
      col("driver_id"),
      col("constructor_id"),
      col("postion_text"),
      col("position_order"),
      col("fastest_lap"),
      col("fastest_lap_time"),
      col("fastest_lap_speed"),
      col("ingestion_date"),
      col("number"),
      col("grid"),
      col("position"),
      col("points"),
      col("laps"),
      col("time"),
      col("miliseconds"),
      col("rank"),
      )

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###create partition by race_id and then push it to the data lake

# COMMAND ----------

results_final_df.write.mode("overwrite").partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.results;

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/ayanstorage0001/processed/results

# COMMAND ----------

df=spark.read.parquet("/mnt/ayanstorage0001/processed/results")

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")
