# Databricks notebook source
# MAGIC %md
# MAGIC ###injest the Constructor json file by defining schema

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/ayanstorage0001/raw/constructors.json

# COMMAND ----------

constructor_schema= "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df= spark.read \
.schema(constructor_schema) \
.json("/mnt/ayanstorage0001/raw/constructors.json")

# COMMAND ----------

constructor_df.printSchema()

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Drop the URL Column from the Constructor DF Table

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

constructor_del_df=constructor_df.drop(col('url'))

# COMMAND ----------

display(constructor_del_df)

# COMMAND ----------

constructor_del_df.schema

# COMMAND ----------

# MAGIC %md
# MAGIC ##column rename and column addtion

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructor_final_df=constructor_del_df \
.withColumnRenamed("constructorId","constructor_id") \
.withColumnRenamed("constructorRef","constructor_ref") \
.withColumn("ingestion_date", current_timestamp())


# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###write that data to parquet file

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from f1_processed.constructors;

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/ayanstorage0001/processed/constructors

# COMMAND ----------

df=spark.read.parquet("/mnt/ayanstorage0001/processed/constructors")

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")
