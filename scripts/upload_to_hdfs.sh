#!/bin/bash

set -e

echo "Creating HDFS directories..."

docker exec namenode hdfs dfs -mkdir -p /data/raw/sales
docker exec namenode hdfs dfs -mkdir -p /data/warehouse/sales_analytics
docker exec namenode hdfs dfs -mkdir -p /tmp/hive

echo ""Uploading CSV files to NameNode container...""

docker cp datasets/sales_transactions.csv namenode:/tmp/sales_transactions.csv
docker cp datasets/Products.csv namenode:/tmp/products.csv
docker cp datasets/categories.csv namenode:/tmp/categories.csv
docker cp datasets/Regions.csv namenode:/tmp/regions.csv

echo "Putting files into HDFS..."

docker exec namenode hdfs dfs -put -f /tmp/sales_transactions.csv /data/raw/sales/
docker exec namenode hdfs dfs -put -f /tmp/products.csv /data/raw/sales/
docker exec namenode hdfs dfs -put -f /tmp/categories.csv /data/raw/sales/
docker exec namenode hdfs dfs -put -f /tmp/regions.csv /data/raw/sales/

echo "HDFS files:"
docker exec namenode hdfs dfs -ls -R /data/raw/sales

echo "Upload completed successfully."