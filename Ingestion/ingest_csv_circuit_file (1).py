# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step1 : read the file with spark dataframe reader

# COMMAND ----------

circuits_df = spark.read \
.option("header", True) \
.option("inferSchema", True) \
.csv("dbfs:/mnt/formula1ayansa0001/raw/circuits.csv")

# COMMAND ----------

dbutils.fs.mounts()  

# COMMAND ----------

# MAGIC %md
# MAGIC ##look through the file system of the path

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###describing the schema

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###checking the min max value to determine the correct datatype

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###manually setiing up the attribute datatype

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Manually inserting the schema type

# COMMAND ----------

circuits_schema=StructType(fields=[StructField("circuitId",IntegerType(),False),
                                   StructField("circuitRef",StringType(),False),
                                   StructField("name",StringType(),False),
                                   StructField("location",StringType(),False),
                                   StructField("country",StringType(),False),
                                   StructField("lat",DoubleType(),False),
                                   StructField("lng",DoubleType(),False),
                                   StructField("alt",IntegerType(),False),
                                   StructField("url",StringType(),False)
                             ])

# COMMAND ----------

circuit_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv("dbfs:/mnt/formula1ayansa0001/raw/circuits.csv")

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Select the important Columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df=circuit_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------


