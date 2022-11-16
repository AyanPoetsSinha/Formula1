# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC ###ingest lap times multiple files

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

# MAGIC %md
# MAGIC ###read using dataframe reader api

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType


# COMMAND ----------

lap_times_schema= StructType(fields=[StructField("raceId",IntegerType(),True),
                                  StructField("driverId",IntegerType(),True),
                                  StructField("position",IntegerType(),True),
                                  StructField("lap",IntegerType(),True),
                                  StructField("time",StringType(),True),
                                  StructField("miliseconds",IntegerType(),True)])

# COMMAND ----------

lap_times_df=spark.read \
.schema(lap_times_schema) \
.csv("/mnt/ayanstorage0001/raw/lap_times/*.csv")

# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

lap_times_final_df=lap_times_df \
.withColumnRenamed("raceId","race_id") \
.withColumnRenamed("driverId","driver_id") \
.withColumn("ingestion_time",current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ### write the processed in output in  processed parquet format

# COMMAND ----------

lap_times_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.laptimes")

# COMMAND ----------

df=spark.read.parquet("/mnt/ayanstorage0001/processed/laptimes")

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")
