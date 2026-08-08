# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

spark

# COMMAND ----------

# MAGIC %md
# MAGIC ##Data Reading

# COMMAND ----------

df = spark.read.format('parquet')\
    .option('inferSchema', 'true')\
    .load('abfss://bronze@carsalesdatalakeishika.dfs.core.windows.net/rawdata')

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Data Transformations

# COMMAND ----------

df=df.withColumn('Model_Category',split(col('Model_ID'),'-')[0])
df.display()

# COMMAND ----------

df.withColumn('Units_sold',col('Units_sold').cast(StringType())).printSchema()
# df.withColumn('Units_sold',col('Units_sold').cast(StringType())).display()

# COMMAND ----------

df=df.withColumn('Revenue_per_unit', col('Revenue')/col('Units_sold'))
df.display()

# COMMAND ----------

df.groupBy(col('Year'), col("Branch_Name")).agg(sum(col('Units_sold')).alias('Total_units_sold'), sum(col('Revenue')).alias('Total_revenue')).orderBy(col('Year'), col('Total_units_sold'), ascending=[True, False]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Data Writing

# COMMAND ----------

df.write.format('parquet').mode('overwrite').option('path','abfss://silver@carsalesdatalakeishika.dfs.core.windows.net/carsales').save()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Querying Silver Layer Data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://silver@carsalesdatalakeishika.dfs.core.windows.net/carsales`;