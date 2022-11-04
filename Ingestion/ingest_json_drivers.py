# Databricks notebook source
dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/ayanstorage0001/raw

# COMMAND ----------

# MAGIC %md
# MAGIC ###Reading the file through Spark Reader API 

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.types import StructType,StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema=StructType(fields=[StructField("forename",StringType(),True),
                               StructField("surname",StringType(),True),   ])

# COMMAND ----------

drivers_schema=StructType(fields=[StructField("driverId",IntegerType(),True),
                               StructField("driverRef",StringType(),True),
                               StructField("number",IntegerType(),True),
                               StructField("code",StringType(),True),
                               StructField("name",name_schema),
                               StructField("dob",DateType(),True),
                               StructField("nationality",StringType(),True),
                               StructField("url",StringType(),True)
                               ]) 

# COMMAND ----------


drivers_df=spark.read \
.schema(drivers_schema) \
.json("/mnt/ayanstorage0001/raw/drivers.json")

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Renaming and Adding new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,col,lit,concat

# COMMAND ----------

drivers_renamed_added_df=drivers_df \
.withColumnRenamed("driverId","driver_id") \
.withColumnRenamed("driverRef","driver_ref") \
.withColumn("ingestion_date",current_timestamp()) \
.withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))

# COMMAND ----------

display(drivers_renamed_added_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Drop the Unwanted columns

# COMMAND ----------

drivers_final_df=drivers_renamed_added_df.drop("url","forname","surname")

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###write the processed file in the output container in the parquet format

# COMMAND ----------

drivers_final_df.write.mode("overwrite").parquet("/mnt/ayanstorage0001/processed/drivers")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/ayanstorage0001/processed/drivers

# COMMAND ----------

df=spark.read.parquet("/mnt/ayanstorage0001/processed/drivers")

# COMMAND ----------

display(df)

# COMMAND ----------


