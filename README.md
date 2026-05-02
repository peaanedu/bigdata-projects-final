## Upload Dataset to HDFS
    chmod +x scripts/upload_to_hdfs.sh
    ./scripts/upload_to_hdfs.sh

## Hive External Tables
    docker cp scripts/create_hive_tables.sql hive-server:/tmp/create_hive_tables.sql

    docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000 -f /tmp/create_hive_tables.sql

## docker exec -it trino trino

docker exec -it trino trino

Use catalog:
    SHOW CATALOGS;
    SHOW SCHEMAS FROM hive;
    SHOW TABLES FROM hive.sales_dw;
### Query 1: Total Sales by Region

## Hive External Tables
scripts/create_sales_dw_hive.sql
docker exec -i hive-server beeline -u jdbc:hive2://localhost:10000 -f /scripts/create_sales_dw_hive.sql

## Cluster Validation Script
    scripts/validate-platform.sh

    # End-to-End Commands
        # 1. Start cluster
    docker compose up -d

    ## 2. Check containers
    docker ps

    ## 3. Generate dataset
    python3 scripts/generate_dataset.py

    ## 4. Upload dataset to HDFS
    chmod +x scripts/upload_to_hdfs.sh
    ./scripts/upload_to_hdfs.sh

    ## 5. Run ETL
    docker exec -it jupyter bash
    cd /home/jovyan/work
    spark-submit etl_sales_to_parquet.py
    exit

    ## 6. Create Hive tables
    docker cp scripts/create_sales_dw_hive.sql hive-server:/tmp/create_sales_dw_hive.sql
    docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000 -f /tmp/create_sales_dw_hive.sql

    ## 7. Test Trino
    docker exec -it trino trino
    SHOW TABLES FROM hive.sales_dw;
    SELECT * FROM hive.sales_dw.fact_sales LIMIT 10;

    ## 8. Validate platform
        chmod +x scripts/validate-platform.sh
        ./scripts/validate-platform.sh

# Demo Script
    1. Introduce project problem
    2. Show Docker Compose architecture
    3. Start cluster:
    docker compose up -d

    4. Show running containers:
    docker ps

    5. Generate dataset:
    python3 scripts/generate_dataset.py
    docker exec -it jupyter python /home/jovyan/work/etl_sales_to_parquet.py
    docker exec namenode hdfs dfs -ls -R /data/warehouse/sales_analytics

    6. Upload data to HDFS:
    ./scripts/upload_to_hdfs.sh

    7. Show files in HDFS:
    docker exec namenode hdfs dfs -ls -R /data/raw/sales

    8. Run PySpark ETL:
    docker exec -it jupyter bash
    spark-submit /home/jovyan/work/etl_sales_to_parquet.py

    9. Show Parquet warehouse:
    docker exec namenode hdfs dfs -ls -R /data/warehouse/sales_analytics

    10. Create Hive tables:
        beeline -u jdbc:hive2://localhost:10000 -f /tmp/create_sales_dw_hive.sql

    11. Run Trino query:
        SELECT region_name, SUM(sales_amount)
        FROM hive.sales_dw.fact_sales
        GROUP BY region_name;
        CMD : ODBC Data source 
        Make sure ZappySys JDBC had install
        - configure 
    12. Open Power BI dashboard
    configure ODBC connection:
         Connection string: jdbc:trino://150.1.7.99:8081/hive/sales_dw
         JDBC Drive file:  C:\Users\Pea AN\Downloads\trino-jdbc-480.jar
         user name: hive
         User password :[]
    connect to trino
        run this command 
            CREATE TABLE hive.sales_dw.fact_sales_analytics AS
            SELECT
                CAST(s.salesid AS INTEGER) AS sales_id,
                CAST(s.productid AS INTEGER) AS product_id,
                p.productname AS product_name,
                CAST(p.categoryid AS INTEGER) AS category_id,
                c.categoryname AS category_name,
                CAST(s.regionid AS INTEGER) AS region_id,
                r.regionname AS region_name,
                CAST(s.quantitysold AS INTEGER) AS quantity_sold,
                CAST(s.salesamount AS DOUBLE) AS sales_amount,
                CAST(s.salesdate AS DATE) AS sales_date,
                year(CAST(s.salesdate AS DATE)) AS sales_year,
                month(CAST(s.salesdate AS DATE)) AS sales_month
            FROM hive.sales_dw.raw_sales_transactions s
            LEFT JOIN hive.sales_dw.raw_products p
                ON CAST(s.productid AS INTEGER) = CAST(p.productid AS INTEGER)
            LEFT JOIN hive.sales_dw.raw_categories c
                ON CAST(p.categoryid AS INTEGER) = CAST(c.categoryid AS INTEGER)
            LEFT JOIN hive.sales_dw.raw_regions r
                ON CAST(s.regionid AS INTEGER) = CAST(r.regionid AS INTEGER);

    12.1 verify table
        SHOW TABLES FROM hive.sales_dw;
        SELECT COUNT(*) FROM hive.sales_dw.fact_sales_analytics;
    12.2 On Power BI Connector ODBC-JDBC
        on Addvance option SQL:
            SELECT
                sales_year,
                sales_month,
                region_name,
                category_name,
                product_name,
                SUM(sales_amount) AS total_sales,
                SUM(quantity_sold) AS total_quantity
            FROM hive.sales_dw.fact_sales_analytics
            GROUP BY
                sales_year,
                sales_month,
                region_name,
                category_name,
                product_name
            ORDER BY
                sales_year,
                sales_month

    13. Explain KPIs and business insights
    14. Conclude with benefits and future improvements
