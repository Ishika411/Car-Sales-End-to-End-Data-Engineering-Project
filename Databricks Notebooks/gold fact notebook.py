# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *

# COMMAND ----------

df_silver = spark.sql("select * from parquet.`abfss://silver@carsalesdatalakeishika.dfs.core.windows.net/carsales`")
df_silver.display()

# COMMAND ----------

df_dealer=spark.sql("select * from car_sales_catalog.gold.dim_dealer")
df_model=spark.sql("select * from car_sales_catalog.gold.dim_model")
df_branch=spark.sql("select * from car_sales_catalog.gold.dim_branch")
df_date=spark.sql("select * from car_sales_catalog.gold.dim_date")

# COMMAND ----------

df_fact=df_silver.join(df_dealer, df_silver['Dealer_ID']==df_dealer['Dealer_ID'], 'left')\
                 .join(df_model, df_silver['Model_ID']==df_model['model_ID'], 'left')\
                 .join(df_branch, df_silver['Branch_ID']==df_branch['branch_ID'], 'left')\
                 .join(df_date, df_silver['Date_ID']==df_date['Date_ID'], 'left')\
                 .select(df_silver['Revenue'], df_silver['Units_sold'], df_silver['Revenue_per_unit'], df_dealer['dim_dealer_key'], df_model['dim_model_key'], df_branch['dim_branch_key'], df_date['dim_date_key'])
df_fact.display()

# COMMAND ----------

if spark.catalog.tableExists('car_sales_catalog.gold.fact_car_sales'):
    delta_tbl = DeltaTable.forName(spark, 'car_sales_catalog.gold.fact_car_sales')
    delta_tbl.alias('t').merge(df_fact.alias('s'), 't.dim_date_key = s.dim_date_key and t.dim_branch_key = s.dim_branch_key and t.dim_model_key = s.dim_model_key and t.dim_dealer_key = s.dim_dealer_key')\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
else:
    df_fact.write.format('delta')\
        .mode('overwrite')\
        .option('path','abfss://gold@carsalesdatalakeishika.dfs.core.windows.net/fact_car_sales')\
        .saveAsTable('car_sales_catalog.gold.fact_car_sales')

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from car_sales_catalog.gold.fact_car_sales