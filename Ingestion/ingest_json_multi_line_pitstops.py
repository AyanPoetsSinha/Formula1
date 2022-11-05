# Databricks notebook source
# MAGIC %md
# MAGIC ###ingest pit stop multiline json file

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

# MAGIC %md
# MAGIC ###read using dataframe reader api

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType


# COMMAND ----------

pitstop_schema= StructType(fields=[StructField("raceId",IntegerType(),True),
                                  StructField("driverId",IntegerType(),True),
                                  StructField("stop",StringType(),True),
                                  StructField("lap",IntegerType(),True),
                                  StructField("time",StringType(),True),
                                  StructField("duration",StringType(),True),
                                  StructField("miliseconds",IntegerType(),True)])

# COMMAND ----------

pitstop_df=spark.read \
.schema(pitstop_schema) \
.option("multiline", True) \
.json("/mnt/ayanstorage0001/raw/pit_stops.json")

# COMMAND ----------

display(pitstop_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

pitstop_final_df=pitstop_df \
.withColumnRenamed("raceId","race_id") \
.withColumnRenamed("driverId","driver_id") \
.withColumn("ingestion_time",current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ### write the processed in output in  processed parquet format

# COMMAND ----------

pitstop_final_df.write.mode("overwrite").parquet("/mnt/ayanstorage0001/processed/pit_stops")

# COMMAND ----------

df=spark.read.parquet("/mnt/ayanstorage0001/processed/pit_stops")

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")
