# Databricks notebook source
# MAGIC %md
# MAGIC - Write data to delta lake - managed table
# MAGIC - Write data to delta lake - external table
# MAGIC - read data from delta lake- table
# MAGIC - read data from delta lake- file

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists f1_demo
# MAGIC location "/mnt/ayanstorage0001/demo"

# COMMAND ----------

results_df= spark.read \
.option("inferSchema", True) \
.json("/mnt/ayanstorage0001/raw/results.json")

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed;

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").save("/mnt/ayanstorage0001/demo/results_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table f1_demo.results_external
# MAGIC using delta
# MAGIC location "/mnt/ayanstorage0001/demo/results_external"

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from f1_demo.results_external;

# COMMAND ----------

results_external_df=spark.read.format("delta").load("/mnt/ayanstorage0001/demo/results_external")

# COMMAND ----------

display(results_external_df)

# COMMAND ----------

# MAGIC %md
# MAGIC - update delta table
# MAGIC -  delete delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC update f1_demo.results_managed 
# MAGIC SET points= 11-position
# MAGIC where position <=10

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from f1_demo.results_managed ;

# COMMAND ----------


