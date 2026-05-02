from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, quarter, when, lit, count, sum as spark_sum, round

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

sales_df = spark.read.option("header", True).option("inferSchema", True).csv(f"{base_path}/sales_transactions.csv")
products_df = spark.read.option("header", True).option("inferSchema", True).csv(f"{base_path}/products.csv")
categories_df = spark.read.option("header", True).option("inferSchema", True).csv(f"{base_path}/categories.csv")
regions_df = spark.read.option("header", True).option("inferSchema", True).csv(f"{base_path}/regions.csv")

sales_clean = (
    sales_df
    .withColumn("SalesID", col("SalesID").cast("int"))
    .withColumn("ProductID", col("ProductID").cast("int"))
    .withColumn("RegionID", col("RegionID").cast("int"))
    .withColumn("QuantitySold", col("QuantitySold").cast("int"))
    .withColumn("SalesAmount", col("SalesAmount").cast("double"))
    .withColumn("SalesDate", to_date(col("SalesDate"), "yyyy-MM-dd"))
    .dropDuplicates(["SalesID"])
    .filter(col("SalesID").isNotNull())
    .filter(col("ProductID").isNotNull())
    .filter(col("RegionID").isNotNull())
    .filter(col("SalesDate").isNotNull())
    .filter(col("QuantitySold") > 0)
    .filter(col("SalesAmount") >= 0)
    .withColumn("SalesYear", year(col("SalesDate")))
    .withColumn("SalesMonth", month(col("SalesDate")))
    .withColumn("SalesQuarter", quarter(col("SalesDate")))
)

products_clean = (
    products_df
    .withColumn("ProductID", col("ProductID").cast("int"))
    .withColumn("CategoryID", col("CategoryID").cast("int"))
    .dropDuplicates(["ProductID"])
)

categories_clean = (
    categories_df
    .withColumn("CategoryID", col("CategoryID").cast("int"))
    .dropDuplicates(["CategoryID"])
)

regions_clean = (
    regions_df
    .withColumn("RegionID", col("RegionID").cast("int"))
    .dropDuplicates(["RegionID"])
)

sales_enriched = (
    sales_clean.alias("s")
    .join(products_clean.alias("p"), col("s.ProductID") == col("p.ProductID"), "left")
    .join(categories_clean.alias("c"), col("p.CategoryID") == col("c.CategoryID"), "left")
    .join(regions_clean.alias("r"), col("s.RegionID") == col("r.RegionID"), "left")
    .select(
        col("s.SalesID"),
        col("s.SalesDate"),
        col("s.SalesYear"),
        col("s.SalesMonth"),
        col("s.SalesQuarter"),
        col("s.ProductID"),
        col("p.ProductName"),
        col("p.CategoryID"),
        col("c.CategoryName"),
        col("s.RegionID"),
        col("r.RegionName"),
        col("s.QuantitySold"),
        col("s.SalesAmount")
    )
    .withColumn("ProductName", when(col("ProductName").isNull(), lit("Unknown Product")).otherwise(col("ProductName")))
    .withColumn("CategoryName", when(col("CategoryName").isNull(), lit("Unknown Category")).otherwise(col("CategoryName")))
    .withColumn("RegionName", when(col("RegionName").isNull(), lit("Unknown Region")).otherwise(col("RegionName")))
)

sales_enriched.select(
    count("*").alias("total_rows"),
    spark_sum(when(col("ProductName") == "Unknown Product", 1).otherwise(0)).alias("missing_product"),
    spark_sum(when(col("CategoryName") == "Unknown Category", 1).otherwise(0)).alias("missing_category"),
    spark_sum(when(col("RegionName") == "Unknown Region", 1).otherwise(0)).alias("missing_region")
).show(truncate=False)

(
    sales_enriched
    .repartition("SalesYear", "SalesMonth")
    .write
    .mode("overwrite")
    .partitionBy("SalesYear", "SalesMonth")
    .parquet(f"{warehouse_path}/fact_sales")
)

products_clean.write.mode("overwrite").parquet(f"{warehouse_path}/dim_products")
categories_clean.write.mode("overwrite").parquet(f"{warehouse_path}/dim_categories")
regions_clean.write.mode("overwrite").parquet(f"{warehouse_path}/dim_regions")

print("Sample Fact Sales Data:")
sales_enriched.show(10, truncate=False)

print("Sales by Region:")
sales_enriched.groupBy("RegionName").agg(
    round(spark_sum("SalesAmount"), 2).alias("TotalSales")
).orderBy(col("TotalSales").desc()).show(truncate=False)

print("Monthly Sales Trend:")
sales_enriched.groupBy("SalesYear", "SalesMonth").agg(
    round(spark_sum("SalesAmount"), 2).alias("MonthlySales")
).orderBy("SalesYear", "SalesMonth").show(truncate=False)

print("ETL completed successfully.")
print("Warehouse path:", warehouse_path)

spark.stop()