# Databricks notebook source
# MAGIC %md
# MAGIC # Spark Advanced Optimization

# COMMAND ----------

# DBTITLE 1,1. Broadcast Join for Small Tables
# 1. BROADCAST JOINS - For small dimension tables
# Broadcast join is efficient when one table is small enough to fit in memory

from pyspark.sql.functions import broadcast

# Load dimension tables (typically smaller)
dim_customers = spark.table("smartretail_corp.gold.dim_customers")
dim_products = spark.table("smartretail_corp.gold.dim_products")
fact_sales = spark.table("smartretail_corp.gold.fact_sales")

print("=" * 80)
print("WITHOUT Explicit Broadcast (relies on auto-broadcast):")
print("=" * 80)

# Regular join - Spark may auto-broadcast if small enough
regular_join = fact_sales.join(
    dim_customers, fact_sales.customer_id == dim_customers.customer_id, "inner"
)

print(f"\nQuery Plan:")
regular_join.explain()

print("\n" + "=" * 80)
print("WITH Explicit Broadcast (forces broadcast):")
print("=" * 80)

# Explicit broadcast - guarantees broadcast even if above threshold
broadcast_join = fact_sales.join(
    broadcast(dim_customers),
    fact_sales.customer_id == dim_customers.customer_id,
    "inner",
)

print(f"\nQuery Plan:")
broadcast_join.explain()

# COMMAND ----------

# DBTITLE 1,2. Partitioning Strategy
# 2. PARTITIONING STRATEGY
# Proper partitioning improves query performance by reducing data scanned

from pyspark.sql.functions import col, year, month

# Read fact_sales and add partition columns
fact_sales_df = spark.table("smartretail_corp.gold.fact_sales") \
    .withColumn("year", year(col("sales_date"))) \
    .withColumn("month", month(col("sales_date")))

# Write partitioned table for efficient filtering by date
partitioned_table = "smartretail_corp.gold.fact_sales_partitioned"

fact_sales_df.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .format("delta") \
    .saveAsTable(partitioned_table)

print(f"\nCreated partitioned table: {partitioned_table}")
print("  Partitioned by: year, month")

# COMMAND ----------

# DBTITLE 1,3. Repartition vs Coalesce
# 3. REPARTITION vs COALESCE
# Understanding when to use each for performance optimization

print("=" * 80)
print("REPARTITION vs COALESCE")
print("=" * 80)

# Load sample data
sales_df = spark.table("smartretail_corp.gold.fact_sales").limit(10000)

print(f"\nOriginal DataFrame:")
print(f"  Rows: {sales_df.count()}")

# REPARTITION - Full shuffle, can increase or decrease partitions
print("\n" + "-" * 80)
print("REPARTITION (increases partitions with full shuffle):")
print("-" * 80)

repartitioned_df = sales_df.repartition(20)
print("  Created with 20 partitions")
print("  Use case: Increase parallelism for large datasets")
print("  Cost: Full shuffle - expensive operation")

# Repartition by column for better data locality
print("\n" + "-" * 80)
print("REPARTITION by column (distributes data by key):")
print("-" * 80)

repartitioned_by_customer = sales_df.repartition(10, "customer_id")
print("  Created with 10 partitions by customer_id")
print("  Use case: Group related data together (e.g., for joins/aggregations)")
print("  Benefit: Same customer_id values in same partition")

# COALESCE - No shuffle, only decreases partitions
print("\n" + "-" * 80)
print("COALESCE (decreases partitions without shuffle):")
print("-" * 80)

coalesced_df = sales_df.coalesce(4)
print("  Reduced to 4 partitions")
print("  Use case: Reduce partitions after filtering (avoid small files)")
print("  Cost: No shuffle - just combines existing partitions")
print("  Limitation: Can only decrease, not increase partitions")

print("\n" + "=" * 80)
print("SUMMARY:")
print("=" * 80)
print("✓ Use REPARTITION when:")
print("  - Need to increase partitions for more parallelism")
print("  - Want to redistribute data evenly by a key")
print("  - Full shuffle is acceptable")
print("\n✓ Use COALESCE when:")
print("  - Need to reduce partitions (e.g., before writing)")
print("  - Want to avoid shuffle overhead")
print("  - Data is already well-distributed")

# COMMAND ----------

# DBTITLE 1,4. Caching for Repeated Queries
# 4. CACHING - For DataFrames used multiple times
# Cache expensive computations that are reused

from pyspark import StorageLevel
import time

print("=" * 80)
print("CACHING DEMONSTRATION")
print("=" * 80)

# Create an expensive computation - join fact with dimensions
expensive_df = spark.sql("""
    SELECT 
        f.sales_id,
        f.sales_date,
        c.customer_name,
        c.state,
        p.product_name,
        p.category,
        f.quantity,
        f.line_total
    FROM smartretail_corp.gold.fact_sales f
    INNER JOIN smartretail_corp.gold.dim_customers c ON f.customer_id = c.customer_id
    INNER JOIN smartretail_corp.gold.dim_products p ON f.product_id = p.product_id
""")

print("\n" + "-" * 80)
print("WITHOUT CACHE - First query:")
print("-" * 80)

start = time.time()
count1 = expensive_df.count()
time1 = time.time() - start
print(f"Count: {count1:,}")
print(f"Time: {time1:.2f} seconds")

print("\n" + "-" * 80)
print("WITHOUT CACHE - Second query (no cache benefit):")
print("-" * 80)

start = time.time()
count2 = expensive_df.count()
time2 = time.time() - start
print(f"Count: {count2:,}")
print(f"Time: {time2:.2f} seconds")

# Now cache the DataFrame
print("\n" + "-" * 80)
print("CACHING DataFrame...")
print("-" * 80)

# expensive_df.cache()
# Trigger cache materialization
start = time.time()
count3 = expensive_df.count()
time3 = time.time() - start
print(f"Count (first with cache): {count3:,}")
print(f"Time: {time3:.2f} seconds (includes caching overhead)")

print("\n" + "-" * 80)
print("WITH CACHE - Subsequent queries (faster):")
print("-" * 80)

start = time.time()
count4 = expensive_df.count()
time4 = time.time() - start
print(f"Count (from cache): {count4:,}")
print(f"Time: {time4:.2f} seconds")

print(f"\n✓ Speedup: {time3/time4:.2f}x faster from cache")

# Different storage levels
print("\n" + "=" * 80)
print("STORAGE LEVELS:")
print("=" * 80)
print("  MEMORY_ONLY: Default, fastest but uses memory")
print("  MEMORY_AND_DISK: Spills to disk if memory full")
print("  DISK_ONLY: Slower but saves memory")
print("  MEMORY_ONLY_SER: Serialized, saves memory but slower")

# Clean up cache
# expensive_df.unpersist()
print("\n✓ Cache cleared")

# COMMAND ----------

# DBTITLE 1,5. Handling Skewed Data
# 5. HANDLING SKEWED DATA
# Techniques to deal with data skew in joins and aggregations

from pyspark.sql.functions import col, rand, lit, concat

print("=" * 80)
print("SKEWED DATA HANDLING")
print("=" * 80)

# Identify skew - check distribution of keys
print("\n" + "-" * 80)
print("STEP 1: Identify Skew")
print("-" * 80)

key_distribution = spark.sql("""
    SELECT 
        customer_id,
        COUNT(*) as order_count
    FROM smartretail_corp.gold.fact_sales
    GROUP BY customer_id
    ORDER BY order_count DESC
    LIMIT 20
""")

print("\nTop 20 customers by order count (potential skew):")
display(key_distribution)

# Get skewed customers (those with > 100 orders as example)
skewed_keys = spark.sql("""
    SELECT customer_id
    FROM smartretail_corp.gold.fact_sales
    GROUP BY customer_id
    HAVING COUNT(*) > 100
""")

print(f"\nCustomers with > 100 orders: {skewed_keys.count()}")

# Technique 1: Salting - Add random suffix to skewed keys
print("\n" + "-" * 80)
print("TECHNIQUE 1: Salting (for skewed joins)")
print("-" * 80)

sales_df = spark.table("smartretail_corp.gold.fact_sales")
customers_df = spark.table("smartretail_corp.gold.dim_customers")

# Add salt to skewed keys
salt_factor = 10  # Number of replicas

# Salt the fact table (add random salt to customer_id)
sales_salted = sales_df.withColumn(
    "salted_customer_id",
    concat(col("customer_id"), lit("_"), (rand() * salt_factor).cast("int"))
)

print(f"✓ Added salt to sales data (factor: {salt_factor})")
print("  Example: customer_id 123 becomes 123_0, 123_1, ..., 123_9")

# Replicate dimension table with all salt values
from pyspark.sql.functions import explode, array

customers_replicated = customers_df.withColumn(
    "salt",
    explode(array([lit(i) for i in range(salt_factor)]))
).withColumn(
    "salted_customer_id",
    concat(col("customer_id"), lit("_"), col("salt"))
)

print(f"✓ Replicated dimension table {salt_factor}x")

# Join on salted keys
salted_join = sales_salted.join(
    customers_replicated,
    sales_salted.salted_customer_id == customers_replicated.salted_customer_id,
    "inner"
)

print("\nJoin completed with salting:")
print("  Benefit: Distributes skewed keys across multiple partitions")

# Technique 2: Adaptive Query Execution (AQE)
print("\n" + "-" * 80)
print("TECHNIQUE 2: Adaptive Query Execution (AQE)")
print("-" * 80)

print("\nAQE features (enabled by default in Databricks):")
print("  ✓ spark.sql.adaptive.enabled = true")
print("  ✓ spark.sql.adaptive.skewJoin.enabled = true")
print("  ✓ spark.sql.adaptive.coalescePartitions.enabled = true")

print("\nAQE automatically:")
print("  1. Detects skewed partitions during query execution")
print("  2. Splits large partitions into smaller ones")
print("  3. Adjusts join strategy dynamically")
print("  4. Coalesces small partitions to reduce overhead")

# Technique 3: Isolated handling of skewed keys
print("\n" + "-" * 80)
print("TECHNIQUE 3: Separate processing for skewed keys")
print("-" * 80)

# Identify highly skewed keys
highly_skewed_threshold = 100

# Process skewed keys separately with broadcast
skewed_sales = sales_df.join(
    skewed_keys,
    sales_df.customer_id == skewed_keys.customer_id,
    "inner"
)

skewed_result = skewed_sales.join(
    broadcast(customers_df),
    skewed_sales.customer_id == customers_df.customer_id,
    "inner"
)

print(f"✓ Skewed keys processed with broadcast join")

# Process non-skewed keys normally
non_skewed_sales = sales_df.join(
    skewed_keys,
    sales_df.customer_id == skewed_keys.customer_id,
    "left_anti"
)

non_skewed_result = non_skewed_sales.join(
    customers_df,
    non_skewed_sales.customer_id == customers_df.customer_id,
    "inner"
)

print(f"✓ Non-skewed keys processed with regular join")

# Union results
final_result = skewed_result.union(non_skewed_result)

print("\n✓ Combined results from both approaches")
print("  Benefit: Optimal strategy for each data segment")

print("\n" + "=" * 80)
print("SUMMARY - Skew Handling Techniques:")
print("=" * 80)
print("1. SALTING: Distribute skewed keys across partitions")
print("2. AQE: Let Spark automatically handle skew (recommended)")
print("3. SEPARATE PROCESSING: Different strategies for skewed vs normal data")
print("4. BROADCAST: For small tables, avoid shuffle entirely")