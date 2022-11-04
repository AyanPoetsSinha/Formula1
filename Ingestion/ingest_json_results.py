# Databricks notebook source
dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls 

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create a schema

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,FloatType

# COMMAND ----------

# MAGIC %md
# MAGIC ###attach the table with schema and read it

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ###Change the attribute name, add new column with time and drop the unnecessary columns

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ###create partition by race_id and then push it to the data lake

# COMMAND ----------


