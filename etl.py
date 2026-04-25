import sys
import urllib.request
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, avg, count, sum as spark_sum, to_date

def get_spark():
    pg_jar = "/home/jovyan/work/jars/postgresql-42.6.0.jar"
    ch_jar = "/home/jovyan/work/jars/clickhouse-jdbc-0.9.8-all.jar" 
    
    return SparkSession.builder \
        .appName("Lab2_ETL_Clean") \
        .config("spark.jars", f"{pg_jar},{ch_jar}") \
        .config("spark.driver.extraClassPath", f"{pg_jar}:{ch_jar}") \
        .config("spark.executor.extraClassPath", f"{pg_jar}:{ch_jar}") \
        .getOrCreate()

def load_csv_to_postgres(spark):
    df_raw = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/jovyan/work/data/mock_data_*.csv")
    
    if df_raw.count() == 0:
        raise Exception("No data read from CSV")

    url_pg = "jdbc:postgresql://postgres_db:5432/bigdata_lab"
    props_pg = {"user": "user", "password": "password", "driver": "org.postgresql.Driver"}
    
    df_raw.write.jdbc(url=url_pg, table="raw_mock_data", mode="overwrite", properties=props_pg)

def build_star_schema(spark):
    url_pg = "jdbc:postgresql://postgres_db:5432/bigdata_lab"
    props_pg = {"user": "user", "password": "password", "driver": "org.postgresql.Driver"}
    
    df = spark.read.jdbc(url=url_pg, table="raw_mock_data", properties=props_pg)
    df_with_date = df.withColumn("sale_date_dt", to_date(col("sale_date")))
    
    dim_product = df.select(
        "sale_product_id", "product_name", "product_category", "product_brand",
        "product_price", "product_rating", "product_reviews", "supplier_name", "supplier_country"
    ).withColumnRenamed("sale_product_id", "product_key") \
     .withColumn("product_price", col("product_price").cast("double")) \
     .withColumn("product_rating", col("product_rating").cast("double")) \
     .withColumn("product_reviews", col("product_reviews").cast("integer")) \
     .dropDuplicates(["product_key"])
    
    dim_customer = df.select(
        "sale_customer_id", "customer_first_name", "customer_last_name",
        "customer_age", "customer_country", "customer_email"
    ).withColumnRenamed("sale_customer_id", "customer_key") \
     .withColumn("customer_age", col("customer_age").cast("integer")) \
     .dropDuplicates(["customer_key"])
    
    dim_store = df.select(
        "store_name", "store_city", "store_state", "store_country", "store_location"
    ).withColumnRenamed("store_name", "store_key") \
     .dropDuplicates(["store_key"])
    
    dim_date = df_with_date.select(
        "sale_date_dt", 
        year("sale_date_dt").alias("year"),
        month("sale_date_dt").alias("month")
    ).withColumnRenamed("sale_date_dt", "date_key") \
     .dropDuplicates(["date_key"])
    
    fact_sales = df_with_date.select(
        "id", "sale_customer_id", "sale_product_id", "store_name", "sale_date_dt",
        "sale_quantity", "sale_total_price", "product_price"
    ).withColumnRenamed("id", "sale_id") \
     .withColumnRenamed("sale_customer_id", "customer_key") \
     .withColumnRenamed("sale_product_id", "product_key") \
     .withColumnRenamed("store_name", "store_key") \
     .withColumnRenamed("sale_date_dt", "date_key") \
     .withColumnRenamed("sale_quantity", "quantity") \
     .withColumnRenamed("sale_total_price", "total_revenue") \
     .withColumnRenamed("product_price", "unit_price") \
     .withColumn("quantity", col("quantity").cast("integer")) \
     .withColumn("total_revenue", col("total_revenue").cast("double")) \
     .withColumn("unit_price", col("unit_price").cast("double"))

    dims = [dim_product, dim_customer, dim_store, dim_date]
    tables = ["dim_product", "dim_customer", "dim_store", "dim_date"]
    
    for d, t in zip(dims, tables):
        d.write.jdbc(url=url_pg, table=t, mode="overwrite", properties=props_pg)
        
    fact_sales.write.jdbc(url=url_pg, table="fact_sales", mode="overwrite", properties=props_pg)
    
    return fact_sales, dim_product, dim_customer, dim_store, dim_date

def init_clickhouse_tables():
    url = "http://clickhouse:8123/"
    queries = [
        "CREATE DATABASE IF NOT EXISTS reports",
        "CREATE TABLE IF NOT EXISTS reports.rpt_products (product_key String, product_name String, product_category String, product_rating Float64, product_reviews Int32, total_qty Int64, total_rev Float64, avg_rat Float64) ENGINE = MergeTree() ORDER BY product_key",
        "CREATE TABLE IF NOT EXISTS reports.rpt_customers (customer_key String, customer_first_name String, customer_last_name String, customer_country String, total_spent Float64, orders_count Int64, avg_check Float64) ENGINE = MergeTree() ORDER BY customer_key",
        "CREATE TABLE IF NOT EXISTS reports.rpt_time (year Int32, month Int32, monthly_rev Float64, orders_count Int64, avg_order_val Float64) ENGINE = MergeTree() ORDER BY (year, month)",
        "CREATE TABLE IF NOT EXISTS reports.rpt_stores (store_key String, store_city String, store_country String, total_rev Float64, transactions Int64, avg_check Float64) ENGINE = MergeTree() ORDER BY store_key",
        "CREATE TABLE IF NOT EXISTS reports.rpt_suppliers (supplier_name String, supplier_country String, total_rev Float64, avg_price Float64, prod_count Int64) ENGINE = MergeTree() ORDER BY supplier_name",
        "CREATE TABLE IF NOT EXISTS reports.rpt_quality (product_key String, product_name String, product_rating Float64, product_reviews Int32, sold_qty Int64, revenue Float64) ENGINE = MergeTree() ORDER BY product_key"
    ]
    
    for query in queries:
        req = urllib.request.Request(url, data=query.encode('utf-8'))
        urllib.request.urlopen(req)

def write_report(df, table_name):
    url_ch = "jdbc:clickhouse://clickhouse:8123/reports"
    props_ch = {"user": "default", "password": "", "driver": "com.clickhouse.jdbc.ClickHouseDriver"}
    df.write.jdbc(url=url_ch, table=table_name, mode="append", properties=props_ch)

def create_clickhouse_reports(spark, fact, prod, cust, store, date_dim):
    df_full = fact.join(prod, on="product_key", how="left") \
                  .join(cust, on="customer_key", how="left") \
                  .join(store, on="store_key", how="left") \
                  .join(date_dim, on="date_key", how="left")

    write_report(
        df_full.groupBy("product_key", "product_name", "product_category", "product_rating", "product_reviews")
               .agg(spark_sum("quantity").alias("total_qty"), spark_sum("total_revenue").alias("total_rev"), avg("product_rating").alias("avg_rat"))
               .orderBy(col("total_rev").desc()),
        "rpt_products"
    )

    write_report(
        df_full.groupBy("customer_key", "customer_first_name", "customer_last_name", "customer_country")
               .agg(spark_sum("total_revenue").alias("total_spent"), count("sale_id").alias("orders_count"), avg("total_revenue").alias("avg_check"))
               .orderBy(col("total_spent").desc()),
        "rpt_customers"
    )

    write_report(
        df_full.groupBy("year", "month")
               .agg(spark_sum("total_revenue").alias("monthly_rev"), count("sale_id").alias("orders_count"), avg("total_revenue").alias("avg_order_val"))
               .orderBy("year", "month"),
        "rpt_time"
    )

    write_report(
        df_full.groupBy("store_key", "store_city", "store_country")
               .agg(spark_sum("total_revenue").alias("total_rev"), count("sale_id").alias("transactions"), avg("total_revenue").alias("avg_check"))
               .orderBy(col("total_rev").desc()),
        "rpt_stores"
    )

    write_report(
        df_full.groupBy("supplier_name", "supplier_country")
               .agg(spark_sum("total_revenue").alias("total_rev"), avg("unit_price").alias("avg_price"), count("product_key").alias("prod_count"))
               .orderBy(col("total_rev").desc()),
        "rpt_suppliers"
    )

    write_report(
        df_full.groupBy("product_key", "product_name", "product_rating", "product_reviews")
               .agg(spark_sum("quantity").alias("sold_qty"), spark_sum("total_revenue").alias("revenue"))
               .orderBy(col("product_rating").desc()),
        "rpt_quality"
    )

if __name__ == "__main__":
    try:
        spark = get_spark()
        load_csv_to_postgres(spark)
        f, p, c, s, d = build_star_schema(spark)
        init_clickhouse_tables()
        create_clickhouse_reports(spark, f, p, c, s, d)
        print("=== SUCCESS ===")
        spark.stop()
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)