# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Get catalog from widget
# Get catalog name from widget parameter
catalog_name = dbutils.widgets.get("catalog")

# COMMAND ----------

# DBTITLE 1,Create catalog and schemas
spark.sql(f"""
create catalog if not exists {catalog_name};
create schema if not exists {catalog_name}.bronze;
create schema if not exists {catalog_name}.silver;
create schema if not exists {catalog_name}.gold;
""")

# COMMAND ----------

# DBTITLE 1,Load data from CSV to bronze tables
# Load metadata from metadata_config table
metadata_df = spark.read.option("multiLine", True).json(f"/Volumes/{catalog_name}/default/raw_data/metadata_config.json")

# Iterate through each row in the metadata DataFrame and load the data into the corresponding table
for row in metadata_df.collect():
    file_path = row['FilePath']
    target_table = f"{catalog_name}.bronze.{row['TargetTableName']}"
    df = spark.read.option("header", True).csv(file_path)
    filename = file_path.split("/")[-1]
    df = df.withColumn("ingestion_timestamp", current_timestamp()) \
           .withColumn("source_file_name", lit(filename))
    df.write.mode("overwrite").saveAsTable(target_table)

# COMMAND ----------

# DBTITLE 1,Query raw_customers
display(spark.sql(f"select * from {catalog_name}.bronze.raw_customers"))

# COMMAND ----------

# DBTITLE 1,Query raw_order_items
display(spark.sql(f"select * from {catalog_name}.bronze.raw_order_items"))

# COMMAND ----------

# DBTITLE 1,Query raw_orders
display(spark.sql(f"select * from {catalog_name}.bronze.raw_orders"))

# COMMAND ----------

# DBTITLE 1,Query raw_products
display(spark.sql(f"select * from {catalog_name}.bronze.raw_products where product_id = 2350"))

# COMMAND ----------

