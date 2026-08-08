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

df_src=spark.sql("select distinct model_ID, model_category from parquet.`abfss://silver@carsalesdatalakeishika.dfs.core.windows.net/carsales`")
df_src.display()


# COMMAND ----------

if spark.catalog.tableExists("car_sales_catalog.gold.dim_model"):
    df_sink=spark.sql('''
        select dim_model_key, model_ID, model_category
        from car_sales_catalog.gold.dim_model'''
    ) 
else:
    df_sink=spark.sql('''
        select 1 as dim_model_key, model_ID, model_category
        from parquet.`abfss://silver@carsalesdatalakeishika.dfs.core.windows.net/carsales`
        where 1=0'''
    ) 
df_sink.display()

# COMMAND ----------

df_filter = df_src.join(df_sink, df_src['Model_ID']==df_sink['Model_ID'], 'left').select(df_src['model_ID'], df_src['model_category'], df_sink['dim_model_key'])
df_filter.display()

# COMMAND ----------

df_filter_old = df_filter.filter(df_filter['dim_model_key'].isNotNull())
df_filter_old.display()
df_filter_new = df_filter.filter(df_filter['dim_model_key'].isNull()).select(df_filter['model_ID'], df_filter['model_category'])
df_filter_new.display()

# COMMAND ----------

if inc_flag=='0':
    # initial load
    max_val=1
else:
    max_val_df=spark.sql("select max(dim_model_key) from car_sales_catalog.gold.dim_model ")
    max_val=max_val_df.collect()[0][0] + 1
print(max_val)

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('dim_model_key', max_val + monotonically_increasing_id())
df_filter_new.display()

# COMMAND ----------

df_final = df_filter_new.union(df_filter_old)
df_final.display()

# COMMAND ----------

if spark.catalog.tableExists('car_sales_catalog.gold.dim_model'):
    delta_tbl = DeltaTable.forPath(spark, 'abfss://gold@carsalesdatalakeishika.dfs.core.windows.net/dim_model')
    delta_tbl.alias('trg').merge(df_final.alias('src'), 'trg.dim_model_key = src.dim_model_key')\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
else:
    df_final.write.format('delta')\
        .mode("overwrite")\
        .option('path','abfss://gold@carsalesdatalakeishika.dfs.core.windows.net/dim_model')\
        .saveAsTable("car_sales_catalog.gold.dim_model")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from car_sales_catalog.gold.dim_model