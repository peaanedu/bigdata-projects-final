from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, year, month, quarter, round,
    when, lit, count, sum as spark_sum
)

# =========================================================
# 1. Spark Session
# =========================================================

spark = (
    SparkSession.builder
    .appName("Sales Analytics ETL to Parquet")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

base_path = "hdfs://namenode:9000/data/raw/sales"
warehouse_path = "hdfs://namenode:9000/data/warehouse/sales_analytics"

print("Starting Sales Analytics ETL...")
print(f"Raw data path: {base_path}")
print(f"Warehouse path: {warehouse_path}")

# =========================================================
# 2. Read Raw CSV Files
# =========================================================

sales_df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv(f"{base_path}/sales_transactions.csv")
)

products_df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv(f"{base_path}/products.csv")
)

categories_df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv(f"{base_path}/categories.csv")
)

regions_df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv(f"{base_path}/regions.csv")
)

print("Raw files loaded successfully.")

# =========================================================
# 3. Standardize and Cast Data Types
# =========================================================

sales_df = (
    sales_df
    .withColumn("order_id", col("order_id").cast("int"))
    .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
    .withColumn("product_id", col("product_id").cast("int"))
    .withColumn("region_id", col("region_id").cast("int"))
    .withColumn("quantity", col("quantity").cast("int"))
    .withColumn("unit_price", col("unit_price").cast("double"))
    .withColumn("sales_amount", col("sales_amount").cast("double"))
)

products_df = (
    products_df
    .withColumn("product_id", col("product_id").cast("int"))
    .withColumn("category_id", col("category_id").cast("int"))
    .withColumn("unit_price", col("unit_price").cast("double"))
)

categories_df = (
    categories_df
    .withColumn("category_id", col("category_id").cast("int"))
)

regions_df = (
    regions_df
    .withColumn("region_id", col("region_id").cast("int"))
)

# =========================================================
# 4. Data Cleaning
# =========================================================

sales_clean = (
    sales_df
    .dropDuplicates(["order_id"])
    .filter(col("order_id").isNotNull())
    .filter(col("order_date").isNotNull())
    .filter(col("product_id").isNotNull())
    .filter(col("region_id").isNotNull())
    .filter(col("quantity") > 0)
    .filter(col("unit_price") >= 0)
    .withColumn(
        "sales_amount",
        when(
            col("sales_amount").isNull(),
            round(col("quantity") * col("unit_price"), 2)
        ).otherwise(col("sales_amount"))
    )
    .withColumn("sales_year", year(col("order_date")))
    .withColumn("sales_month", month(col("order_date")))
    .withColumn("sales_quarter", quarter(col("order_date")))
)

products_clean = (
    products_df
    .dropDuplicates(["product_id"])
    .filter(col("product_id").isNotNull())
    .filter(col("category_id").isNotNull())
)

categories_clean = (
    categories_df
    .dropDuplicates(["category_id"])
    .filter(col("category_id").isNotNull())
)

regions_clean = (
    regions_df
    .dropDuplicates(["region_id"])
    .filter(col("region_id").isNotNull())
)

print("Data cleaning completed.")

# =========================================================
# 5. Data Enrichment / Star Schema Join
# =========================================================

sales_enriched = (
    sales_clean.alias("s")
    .join(products_clean.alias("p"), col("s.product_id") == col("p.product_id"), "left")
    .join(categories_clean.alias("c"), col("p.category_id") == col("c.category_id"), "left")
    .join(regions_clean.alias("r"), col("s.region_id") == col("r.region_id"), "left")
    .select(
        col("s.order_id"),
        col("s.order_date"),
        col("s.sales_year"),
        col("s.sales_month"),
        col("s.sales_quarter"),
        col("s.product_id"),
        col("p.product_name"),
        col("p.category_id"),
        col("c.category_name"),
        col("s.region_id"),
        col("r.region_name"),
        col("r.country"),
        col("s.quantity"),
        col("s.unit_price"),
        col("s.sales_amount")
    )
    .withColumn("product_name", when(col("product_name").isNull(), lit("Unknown Product")).otherwise(col("product_name")))
    .withColumn("category_name", when(col("category_name").isNull(), lit("Unknown Category")).otherwise(col("category_name")))
    .withColumn("region_name", when(col("region_name").isNull(), lit("Unknown Region")).otherwise(col("region_name")))
    .withColumn("country", when(col("country").isNull(), lit("Unknown Country")).otherwise(col("country")))
)

print("Data enrichment completed.")

# =========================================================
# 6. Data Quality Summary
# =========================================================

print("Data Quality Summary:")

sales_enriched.select(
    count("*").alias("total_rows"),
    spark_sum(when(col("product_name") == "Unknown Product", 1).otherwise(0)).alias("missing_product"),
    spark_sum(when(col("category_name") == "Unknown Category", 1).otherwise(0)).alias("missing_category"),
    spark_sum(when(col("region_name") == "Unknown Region", 1).otherwise(0)).alias("missing_region")
).show(truncate=False)

# =========================================================
# 7. Write Parquet Warehouse Tables
# =========================================================

(
    sales_enriched
    .repartition("sales_year", "sales_month")
    .write
    .mode("overwrite")
    .partitionBy("sales_year", "sales_month")
    .parquet(f"{warehouse_path}/fact_sales")
)

(
    products_clean
    .write
    .mode("overwrite")
    .parquet(f"{warehouse_path}/dim_products")
)

(
    categories_clean
    .write
    .mode("overwrite")
    .parquet(f"{warehouse_path}/dim_categories")
)

(
    regions_clean
    .write
    .mode("overwrite")
    .parquet(f"{warehouse_path}/dim_regions")
)

print("Parquet warehouse tables written successfully.")

# =========================================================
# 8. Validation Queries
# =========================================================

print("Sample Fact Sales Data:")
sales_enriched.show(10, truncate=False)

print("Sales by Region:")
sales_enriched.groupBy("region_name").agg(
    round(spark_sum("sales_amount"), 2).alias("total_sales")
).orderBy(col("total_sales").desc()).show(truncate=False)

print("Monthly Sales Trend:")
sales_enriched.groupBy("sales_year", "sales_month").agg(
    round(spark_sum("sales_amount"), 2).alias("monthly_sales")
).orderBy("sales_year", "sales_month").show(truncate=False)

print("ETL completed successfully.")
print("Warehouse path:", warehouse_path)

spark.stop()