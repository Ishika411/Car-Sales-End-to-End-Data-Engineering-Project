# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create Flag Parameter

# COMMAND ----------

dbutils.widgets.text("incremental_flag",'0')

# COMMAND ----------

inc_flag=dbutils.widgets.get('incremental_flag')
print(inc_flag)

# COMMAND ----------

df_src=spark.sql("select distinct Dealer_ID, Dealer_Name from parquet.`abfss://silver@carsalesdatalakeishika.dfs.core.windows.net/carsales`")
df_src.display()


# COMMAND ----------

if spark.catalog.tableExists("car_sales_catalog.gold.dim_dealer"):
    df_sink=spark.sql('''
        select dim_dealer_key, Dealer_ID, Dealer_Name
        from car_sales_catalog.gold.dim_dealer '''
    ) 
else:
    df_sink=spark.sql('''
        select 1 as dim_dealer_key, Dealer_ID, Dealer_Name
        from parquet.`abfss://silver@carsalesdatalakeishika.dfs.core.windows.net/carsales`
        where 1=0'''
    ) 
df_sink.display()

# COMMAND ----------

df_filter = df_src.join(df_sink, df_src['Dealer_ID']==df_sink['Dealer_ID'], 'left').select(df_src['Dealer_ID'], df_src['Dealer_Name'], df_sink['dim_dealer_key'])
df_filter.display()

# COMMAND ----------

df_filter_old = df_filter.filter(df_filter['dim_dealer_key'].isNotNull())
df_filter_old.display()
df_filter_new = df_filter.filter(df_filter['dim_dealer_key'].isNull()).select(df_filter['Dealer_ID'], df_filter['Dealer_Name'])
df_filter_new.display()

# COMMAND ----------

if inc_flag=='0':
    # initial load
    max_val=1
else:
    max_val_df=spark.sql("select max(dim_dealer_key) from car_sales_catalog.gold.dim_dealer ")
    max_val=max_val_df.collect()[0][0] + 1
print(max_val)

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('dim_dealer_key', max_val + monotonically_increasing_id())
df_filter_new.display()

# COMMAND ----------

df_final = df_filter_new.union(df_filter_old)
df_final.display()

# COMMAND ----------

if spark.catalog.tableExists('car_sales_catalog.gold.dim_dealer'):
    delta_tbl = DeltaTable.forPath(spark, 'abfss://gold@carsalesdatalakeishika.dfs.core.windows.net/dim_dealer')
    delta_tbl.alias('trg').merge(df_final.alias('src'), 'trg.dim_dealer_key = src.dim_dealer_key')\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
else:
    df_final.write.format('delta')\
        .mode("overwrite")\
        .option('path','abfss://gold@carsalesdatalakeishika.dfs.core.windows.net/dim_dealer')\
        .saveAsTable("car_sales_catalog.gold.dim_dealer")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from car_sales_catalog.gold.dim_dealer