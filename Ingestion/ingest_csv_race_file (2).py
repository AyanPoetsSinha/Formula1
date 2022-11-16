# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step1 : read the file with spark dataframe reader

# COMMAND ----------

races_df = spark.read \
.option("header", True) \
.option("inferSchema", True) \
.csv("dbfs:/mnt/ayanstorage0001/raw/races.csv")

# COMMAND ----------

dbutils.fs.mounts()  

# COMMAND ----------

# MAGIC %md
# MAGIC ##look through the file system of the path

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###describing the schema

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###checking the min max value to determine the correct datatype

# COMMAND ----------

races_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###manually setiing up the attribute datatype

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType, DateType

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Manually inserting the schema type

# COMMAND ----------

races_schema=StructType(fields=[StructField("raceId",IntegerType(),False),
                                   StructField("year",IntegerType(),True),
                                   StructField("round",IntegerType(),True),
                                   StructField("circuitId",IntegerType(),True),
                                   StructField("name",StringType(),True),
                                   StructField("date",StringType(),True),
                                   StructField("time",StringType(),True),
                                   ])

# COMMAND ----------

race_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv("dbfs:/mnt/ayanstorage0001/raw/races.csv")

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Select the important Columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

races_selected_df=races_df.select(col("raceId"),col("year"),col("round"),col("circuitId"),col("name"),col("date"),col("time"))

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###changing the column names

# COMMAND ----------

races_renamed_df=races_selected_df.withColumnRenamed('raceId','race_id') \
.withColumnRenamed('year','race_year') \
.withColumnRenamed('circuitId','circuit_id') 


# COMMAND ----------

display (races_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### add additional column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,to_timestamp,concat,lit,col
#--,lit

# COMMAND ----------

races_final_df=races_renamed_df.withColumn('races_timestamp', to_timestamp (concat (col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
.withColumn('ingestion_date',current_timestamp())
# --\
# --.withColumn('env',lit('Production'))

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Choosing only important column

# COMMAND ----------

races_superfinal_df=races_final_df.select(col("race_id"),col("race_year"),col("round"),col("circuit_id"),col("name"),col("races_timestamp"),col("ingestion_date"))

# COMMAND ----------

display(races_superfinal_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### write the data to datalake using parquet

# COMMAND ----------

races_superfinal_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/ayanstorage0001/processed/races

# COMMAND ----------

df=spark.read.parquet("/mnt/ayanstorage0001/processed/races")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##create partitioin by year in the output and write again

# COMMAND ----------

races_superfinal_df.write.mode("overwrite").partitionBy('race_year').format("parquet").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/ayanstorage0001/processed/races

# COMMAND ----------

df=spark.read.parquet("/mnt/ayanstorage0001/processed/races")

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")
