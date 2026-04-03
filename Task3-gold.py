# Databricks notebook source
# DBTITLE 1,Gold Layer - Dimensional Model
# MAGIC %md
# MAGIC # Gold Layer (Data Modeling)

# COMMAND ----------

# DBTITLE 1,Get catalog from widget
# Get catalog from widget
catalog_name = dbutils.widgets.get("catalog")
print(f"Using catalog: {catalog_name}")

# COMMAND ----------

# DBTITLE 1,Create dim_customers
display(spark.sql(f"""
-- Create customer dimension table
create or replace table {catalog_name}.gold.dim_customers as
select
  customer_id,
  name as customer_name,
  city,
  state,
  signup_date,
  created_at,
  updated_at,
  record_created_at,
  record_updated_at
from {catalog_name}.silver.customers;
"""))

# COMMAND ----------

# DBTITLE 1,Create dim_products
display(spark.sql(f"""
-- Create product dimension table
create or replace table {catalog_name}.gold.dim_products as
select
  product_id,
  product_name,
  category,
  price,
  created_at,
  updated_at,
  record_created_at,
  record_updated_at
from {catalog_name}.silver.products;
"""))

# COMMAND ----------

# DBTITLE 1,Create fact_sales
display(spark.sql(f"""
-- Create sales fact table
create or replace table {catalog_name}.gold.fact_sales as
select
  oi.order_item_id as sales_id,
  o.order_id,
  o.customer_id,
  oi.product_id,
  o.order_date as sales_date,
  oi.quantity,
  oi.price as unit_price,
  (oi.quantity * oi.price) as line_total,
  o.total_amount as order_total,
  o.order_status,
  o.created_at,
  o.updated_at,
  o.record_created_at,
  o.record_updated_at
from {catalog_name}.silver.orders o
inner join {catalog_name}.silver.order_items oi
  on o.order_id = oi.order_id;
"""))

# COMMAND ----------

# DBTITLE 1,Aggregations
# MAGIC %md
# MAGIC Business Aggregations

# COMMAND ----------

# DBTITLE 1,Revenue by State
display(spark.sql(f"""
-- Revenue aggregation by state
create or replace table {catalog_name}.gold.agg_revenue_by_state as
select
  c.state,
  count(distinct f.order_id) as total_orders,
  count(distinct f.customer_id) as unique_customers,
  sum(f.line_total) as total_revenue,
  avg(f.line_total) as avg_order_line_value,
  sum(f.quantity) as total_quantity_sold
from {catalog_name}.gold.fact_sales f
inner join {catalog_name}.gold.dim_customers c
  on f.customer_id = c.customer_id
group by c.state
order by total_revenue desc;
"""))

# COMMAND ----------

# DBTITLE 1,Top Products
display(spark.sql(f"""
-- Top products by revenue and quantity
create or replace table {catalog_name}.gold.agg_top_products as
select
  p.product_id,
  p.product_name,
  p.category,
  count(distinct f.order_id) as times_ordered,
  sum(f.quantity) as total_quantity_sold,
  sum(f.line_total) as total_revenue,
  avg(f.unit_price) as avg_unit_price,
  sum(f.line_total) / sum(f.quantity) as revenue_per_unit
from {catalog_name}.gold.fact_sales f
inner join {catalog_name}.gold.dim_products p
  on f.product_id = p.product_id
group by p.product_id, p.product_name, p.category
order by total_revenue desc;
"""))

# COMMAND ----------

# DBTITLE 1,Daily Sales Trends
display(spark.sql(f"""
-- Daily sales trends
create or replace table {catalog_name}.gold.agg_daily_sales as
select
  sales_date,
  count(distinct order_id) as daily_orders,
  count(distinct customer_id) as daily_customers,
  sum(line_total) as daily_revenue,
  avg(line_total) as avg_line_value,
  sum(quantity) as daily_quantity_sold
from {catalog_name}.gold.fact_sales
group by sales_date
order by sales_date desc;
"""))

# COMMAND ----------

# DBTITLE 1,Monthly Sales Trends
display(spark.sql(f"""
-- Monthly sales trends
create or replace table {catalog_name}.gold.agg_monthly_sales as
select
  year(sales_date) as year,
  month(sales_date) as month,
  date_trunc('month', sales_date) as month_start,
  count(distinct order_id) as monthly_orders,
  count(distinct customer_id) as monthly_customers,
  sum(line_total) as monthly_revenue,
  avg(line_total) as avg_line_value,
  sum(quantity) as monthly_quantity_sold
from {catalog_name}.gold.fact_sales
group by year(sales_date), month(sales_date), date_trunc('month', sales_date)
order by year desc, month desc;
"""))