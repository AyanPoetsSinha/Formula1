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

# MAGIC %md
# MAGIC ###changing the column names

# COMMAND ----------

circuits_renamed_dataframe=circuits_selected_df.withColumnRenamed('circuitId','circuit_Id') \
.withColumnRenamed('circuitRef','circuit_ref') \
.withColumnRenamed('lat','Latitufe') \
.withColumnRenamed('lng','longitude') \
.withColumnRenamed('alt','Altitude')


# COMMAND ----------

display (circuits_renamed_dataframe)

# COMMAND ----------

# MAGIC %md
# MAGIC ### add additional column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp 
#--,lit

# COMMAND ----------

circuits_final_df=circuits_renamed_dataframe \
.withColumn('ingestion_date',current_timestamp()) 
# --\
# --.withColumn('env',lit('Production'))

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### write the data to datalake using parquet

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet("/mnt/formula1ayansa0001/processed/circuits_processed")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1ayansa0001/processed/circuits_processed

# COMMAND ----------

df=spark.read.parquet("/mnt/formula1ayansa0001/processed/circuits_processed")

# COMMAND ----------

display(df)

# COMMAND ----------


