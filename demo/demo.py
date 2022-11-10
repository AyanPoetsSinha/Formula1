# Databricks notebook source
# MAGIC %fs
# MAGIC 
# MAGIC ls /mnt/ayanstorage0001/processed/results/

# COMMAND ----------

df=spark.read.parquet("/mnt/ayanstorage0001/processed/bbc_raceresult")

# COMMAND ----------

display(df)

# COMMAND ----------

df \
.groupBy("driver_name") \
.show()

# COMMAND ----------

f
