# Databricks notebook source
# MAGIC %md
# MAGIC ##Create Catalog
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG car_sales_catalog
# MAGIC MANAGED LOCATION 'abfss://car-sales-container@carsalesdatalakeishika.dfs.core.windows.net/car_sales_catalog';

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create Schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA car_sales_catalog.silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA car_sales_catalog.gold;