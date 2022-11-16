# Databricks notebook source
# MAGIC %md
# MAGIC ###ingest qualifying multi line json data

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

# MAGIC %md
# MAGIC ###read using dataframe reader api

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType


# COMMAND ----------

qualifying_schema= StructType(fields=[StructField("qualifyId",IntegerType(),True),
                                  StructField("raceId",IntegerType(),True),
                                  StructField("driverId",IntegerType(),True),
                                  StructField("constructorId",IntegerType(),True),
                                  StructField("number",IntegerType(),True),
                                  StructField("position",IntegerType(),True),
                                  StructField("q1",StringType(),True),
                                  StructField("q2",StringType(),True),
                                  StructField("q3",StringType(),True)])

# COMMAND ----------

qualifying_df=spark.read \
.schema(qualifying_schema) \
.option("multiline", True) \
.json("/mnt/ayanstorage0001/raw/qualifying")

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------



# COMMAND ----------

qualifying_final_df=qualifying_df \
.withColumnRenamed("raceId","race_id") \
.withColumnRenamed("driverId","driver_id") \
.withColumnRenamed("qualifyId","qualify_id") \
.withColumnRenamed("constructorId","constructor_id") \
.withColumn("ingestion_time",current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ### write the processed in output in  processed parquet format

# COMMAND ----------

qualifying_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

df=spark.read.parquet("/mnt/ayanstorage0001/processed/qualifying")

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")
