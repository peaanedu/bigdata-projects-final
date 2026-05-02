-- =========================================================
-- SALES ANALYTICS DATA WAREHOUSE - HIVE SCRIPT
-- =========================================================

CREATE DATABASE IF NOT EXISTS sales_dw;
USE sales_dw;

-- Recommended Hive settings
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.fetch.column.stats=true;

-- ADD THESE
SET hive.exec.max.dynamic.partitions=1000;
SET hive.exec.max.dynamic.partitions.pernode=500;

-- =========================================================
-- 1. DROP RAW TABLES
-- =========================================================

DROP TABLE IF EXISTS raw_sales_transactions;
DROP TABLE IF EXISTS raw_products;
DROP TABLE IF EXISTS raw_categories;
DROP TABLE IF EXISTS raw_regions;

-- =========================================================
-- 2. RAW EXTERNAL TABLES
-- =========================================================

CREATE EXTERNAL TABLE raw_sales_transactions (
    SalesID INT,
    ProductID INT,
    RegionID INT,
    QuantitySold INT,
    SalesAmount DOUBLE,
    SalesDate STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/raw/sales/sales_transactions'
TBLPROPERTIES (
    "skip.header.line.count"="1"
);

CREATE EXTERNAL TABLE raw_products (
    ProductID INT,
    ProductName STRING,
    CategoryID INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/raw/sales/products'
TBLPROPERTIES (
    "skip.header.line.count"="1"
);

CREATE EXTERNAL TABLE raw_categories (
    CategoryID INT,
    CategoryName STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/raw/sales/categories'
TBLPROPERTIES (
    "skip.header.line.count"="1"
);

CREATE EXTERNAL TABLE raw_regions (
    RegionID INT,
    RegionName STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/raw/sales/regions'
TBLPROPERTIES (
    "skip.header.line.count"="1"
);

-- =========================================================
-- 3. VALIDATION RAW DATA
-- =========================================================

SELECT 'raw_sales_transactions' AS table_name, COUNT(*) AS total_rows FROM raw_sales_transactions
UNION ALL
SELECT 'raw_products', COUNT(*) FROM raw_products
UNION ALL
SELECT 'raw_categories', COUNT(*) FROM raw_categories
UNION ALL
SELECT 'raw_regions', COUNT(*) FROM raw_regions;

-- =========================================================
-- 4. DROP TARGET TABLE
-- =========================================================

DROP TABLE IF EXISTS fact_sales_analytics_hive;

-- =========================================================
-- 5. CREATE OPTIMIZED FACT TABLE
-- Stored as Parquet + Partitioned by Year/Month
-- =========================================================

CREATE TABLE fact_sales_analytics_hive (
    SalesID INT,
    ProductID INT,
    ProductName STRING,
    CategoryID INT,
    CategoryName STRING,
    RegionID INT,
    RegionName STRING,
    QuantitySold INT,
    SalesAmount DOUBLE,
    SalesDate DATE,
    SalesQuarter INT
)
PARTITIONED BY (
    SalesYear INT,
    SalesMonth INT
)
STORED AS PARQUET;

-- =========================================================
-- 6. LOAD TRANSFORMED DATA
-- =========================================================

INSERT OVERWRITE TABLE fact_sales_analytics_hive
PARTITION (SalesYear, SalesMonth)
SELECT
    s.SalesID,
    s.ProductID,
    COALESCE(p.ProductName, 'Unknown Product') AS ProductName,
    p.CategoryID,
    COALESCE(c.CategoryName, 'Unknown Category') AS CategoryName,
    s.RegionID,
    COALESCE(r.RegionName, 'Unknown Region') AS RegionName,
    COALESCE(s.QuantitySold, 0) AS QuantitySold,
    COALESCE(s.SalesAmount, 0.0) AS SalesAmount,
    TO_DATE(s.SalesDate) AS SalesDate,
    QUARTER(TO_DATE(s.SalesDate)) AS SalesQuarter,
    YEAR(TO_DATE(s.SalesDate)) AS SalesYear,
    MONTH(TO_DATE(s.SalesDate)) AS SalesMonth
FROM raw_sales_transactions s
LEFT JOIN raw_products p
    ON s.ProductID = p.ProductID
LEFT JOIN raw_categories c
    ON p.CategoryID = c.CategoryID
LEFT JOIN raw_regions r
    ON s.RegionID = r.RegionID
WHERE s.SalesID IS NOT NULL
  AND s.ProductID IS NOT NULL
  AND s.RegionID IS NOT NULL
  AND s.SalesDate IS NOT NULL;

-- =========================================================
-- 7. COMPUTE STATISTICS FOR QUERY OPTIMIZATION
-- =========================================================

ANALYZE TABLE fact_sales_analytics_hive COMPUTE STATISTICS;
ANALYZE TABLE fact_sales_analytics_hive COMPUTE STATISTICS FOR COLUMNS;

-- =========================================================
-- 8. VALIDATION TARGET TABLE
-- =========================================================

SELECT COUNT(*) AS total_fact_rows
FROM fact_sales_analytics_hive;

SELECT *
FROM fact_sales_analytics_hive
LIMIT 10;

-- =========================================================
-- 9. BUSINESS ANALYTICS QUERIES
-- =========================================================

-- Sales by Region
SELECT
    RegionName,
    ROUND(SUM(SalesAmount), 2) AS TotalSales,
    SUM(QuantitySold) AS TotalQuantity
FROM fact_sales_analytics_hive
GROUP BY RegionName
ORDER BY TotalSales DESC;

-- Sales by Category
SELECT
    CategoryName,
    ROUND(SUM(SalesAmount), 2) AS TotalSales,
    SUM(QuantitySold) AS TotalQuantity
FROM fact_sales_analytics_hive
GROUP BY CategoryName
ORDER BY TotalSales DESC;

-- Monthly Sales Trend
SELECT
    SalesYear,
    SalesMonth,
    ROUND(SUM(SalesAmount), 2) AS MonthlySales
FROM fact_sales_analytics_hive
GROUP BY SalesYear, SalesMonth
ORDER BY SalesYear, SalesMonth;

-- Top 10 Products
SELECT
    ProductName,
    ROUND(SUM(SalesAmount), 2) AS TotalSales,
    SUM(QuantitySold) AS TotalQuantity
FROM fact_sales_analytics_hive
GROUP BY ProductName
ORDER BY TotalSales DESC
LIMIT 10;

-- Data Quality Check
SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN ProductName = 'Unknown Product' THEN 1 ELSE 0 END) AS missing_product,
    SUM(CASE WHEN CategoryName = 'Unknown Category' THEN 1 ELSE 0 END) AS missing_category,
    SUM(CASE WHEN RegionName = 'Unknown Region' THEN 1 ELSE 0 END) AS missing_region
FROM fact_sales_analytics_hive;