# Databricks notebook source
# DBTITLE 1,Delta Lake Optimization
# MAGIC %md
# MAGIC # Delta Lake Optimization

# COMMAND ----------

# DBTITLE 1,Delta Lake Optimization Features
# MAGIC %md
# MAGIC ## 1. OPTIMIZE with Z-ORDER

# COMMAND ----------

# DBTITLE 1,OPTIMIZE with Z-ORDER
# MAGIC %sql
# MAGIC -- OPTIMIZE compacts small files into larger ones for better performance
# MAGIC -- Z-ORDER co-locates related information in the same files for faster queries
# MAGIC
# MAGIC -- Check table details before optimization
# MAGIC DESCRIBE DETAIL smartretail_corp.gold.fact_sales;

# COMMAND ----------

# DBTITLE 1,Run OPTIMIZE with Z-ORDER
# MAGIC %sql
# MAGIC -- OPTIMIZE with Z-ORDER on frequently filtered columns
# MAGIC -- This improves query performance by reducing data scanning
# MAGIC OPTIMIZE smartretail_corp.gold.fact_sales
# MAGIC ZORDER BY (customer_id, product_id, sales_date);

# COMMAND ----------

# DBTITLE 1,Check After OPTIMIZE
# MAGIC %sql
# MAGIC -- Check table after optimization
# MAGIC DESCRIBE DETAIL smartretail_corp.gold.fact_sales;

# COMMAND ----------

# DBTITLE 1,VACUUM
# MAGIC %md
# MAGIC ## 2. VACUUM - Clean Up Old Files

# COMMAND ----------

# DBTITLE 1,VACUUM Command
# MAGIC %sql
# MAGIC -- VACUUM removes old data files that are no longer referenced
# MAGIC -- Default retention is 7 days to allow time travel
# MAGIC -- DRY RUN first to see what would be deleted
# MAGIC VACUUM smartretail_corp.gold.fact_sales RETAIN 168 HOURS DRY RUN;

# COMMAND ----------

# DBTITLE 1,Run VACUUM
# MAGIC %sql
# MAGIC -- Actual VACUUM (removes files older than 7 days)
# MAGIC -- Note: This will prevent time travel beyond the retention period
# MAGIC VACUUM smartretail_corp.gold.fact_sales RETAIN 168 HOURS;

# COMMAND ----------

# DBTITLE 1,Time Travel
# MAGIC %md
# MAGIC ## 3. Time Travel Validation

# COMMAND ----------

# DBTITLE 1,Time Travel Demo
# Query table history
history = spark.sql("DESCRIBE HISTORY smartretail_corp.gold.fact_sales")
print("Table History:")
display(history.select("version", "timestamp", "operation", "operationMetrics").limit(10))

# Get the latest version number
latest_version = history.select("version").first()[0]
print(f"\nLatest version: {latest_version}")

# COMMAND ----------

# DBTITLE 1,Query Historical Versions
# Time travel - query specific version
if latest_version >= 1:
    # Query previous version
    previous_version = spark.read.format("delta").option("versionAsOf", latest_version - 1).table("smartretail_corp.gold.fact_sales")
    previous_count = previous_version.count()
    print(f"Version {latest_version - 1} count: {previous_count}")

# Query current version
current_version = spark.table("smartretail_corp.gold.fact_sales")
current_count = current_version.count()
print(f"Current version {latest_version} count: {current_count}")

# Time travel by timestamp (query as of 1 day ago)
try:
    timestamp_query = spark.read.format("delta").option("timestampAsOf", "2024-01-01").table("smartretail_corp.gold.fact_sales")
    print(f"\nData as of 2024-01-01: {timestamp_query.count()} records")
except Exception as e:
    print(f"\nTime travel by timestamp failed (expected if date is before table creation): {str(e)[:100]}")

# COMMAND ----------

# DBTITLE 1,Schema Enforcement
# MAGIC %md
# MAGIC ## 4. Schema Enforcement

# COMMAND ----------

# DBTITLE 1,Schema Enforcement Demo
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType

# Get current schema
current_schema = spark.table("smartretail_corp.gold.dim_products").schema
print("Current dim_products schema:")
for field in current_schema.fields:
    print(f"  {field.name}: {field.dataType}")

# Try to insert data with incompatible schema (should fail)
print("\n--- Testing Schema Enforcement ---")

# Create data with wrong types
invalid_data = [
    (1, "Product A", "Category X", "invalid_price")  # price should be float, not string
]

invalid_schema = StructType([
    StructField("product_id", IntegerType()),
    StructField("product_name", StringType()),
    StructField("category", StringType()),
    StructField("price", StringType())  # Wrong type!
])

invalid_df = spark.createDataFrame(invalid_data, invalid_schema)

try:
    invalid_df.write.format("delta").mode("append").saveAsTable("smartretail_corp.gold.dim_products")
    print("Write succeeded (unexpected)")
except Exception as e:
    print(f"Schema enforcement prevented incompatible write: {str(e)[:150]}")
    print("\n✓ Schema enforcement is working!")

# COMMAND ----------

# DBTITLE 1,Partition Pruning
# MAGIC %md
# MAGIC ## 5. Partition Pruning

# COMMAND ----------

# DBTITLE 1,Partition Pruning Demo
# Load the partitioned table
fact_sales_partitioned = spark.table("smartretail_corp.gold.fact_sales_partitioned")

# Query WITHOUT partition pruning (scans all partitions)
print("--- Query WITHOUT Partition Filter ---")
query_all = fact_sales_partitioned.groupBy("product_id").count()

# Query WITH partition pruning (scans only specific partitions)
print("\n--- Query WITH Partition Filter (sales_date) ---")
from pyspark.sql.functions import col

query_filtered = fact_sales_partitioned \
    .filter(col("sales_date") == "2023-12-31") \
    .groupBy("product_id").count()

print("Filtered query created (partition pruning will reduce files scanned)")

# Show explain plan to see partition pruning
print("\n--- Explain Plan (shows partition filters) ---")
query_filtered.explain(True)

# COMMAND ----------

# DBTITLE 1,Verify Partition Pruning
# Show the partitions in the table
partitions = spark.sql("SHOW PARTITIONS smartretail_corp.gold.fact_sales_partitioned")
print(f"Total partitions in table: {partitions.count()}")
print("\nSample partitions:")
display(partitions.limit(10))

print("\n✓ When querying with sales_date filter, only relevant partitions are scanned!")
print("This dramatically improves query performance on large tables.")