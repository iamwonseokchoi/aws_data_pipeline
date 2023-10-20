import datetime as dt
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType

from src.processing.spark import get_spark_session, run_iceberg_ddl


# Sales schema
sales_schema = StructType([
    StructField("sale_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("price", FloatType(), True),
    StructField("product", StringType(), True),
    StructField("quantity", LongType(), True),
    StructField("timestamp", StringType(), True)
])

# Iventory schema
inventory_schema = StructType([
    StructField("inventory_id", StringType(), True),
    StructField("product", StringType(), True),
    StructField("stock_count", LongType(), True),
    StructField("timestamp", StringType(), True)
])

# Users schema
users_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("user_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("age", LongType(), True),
    StructField("gender", StringType(), True),
    StructField("registered_on", StringType(), True)
])


if __name__ == "__main__":
    # Scoping
    emr_timestamp = dt.datetime.now().strftime("%Y%m%d%H%M%S")
    spark = get_spark_session(app_name=f'OnDemandBatch-{emr_timestamp}')
    
    # Run DDL (Default False)
    run_iceberg_ddl(run=True, upstream_spark=spark)
    
    # Read raw data from S3
    df_sales = spark.read.schema(sales_schema).json(f"s3a://amore-bucket/raw/sales/*")
    df_inventory = spark.read.schema(inventory_schema).json(f"s3a://amore-bucket/raw/inventory/*")
    df_users = spark.read.schema(users_schema).json(f"s3a://amore-bucket/raw/user/*")
    
    # Create Temporary Views for Iceberg Merge Syntax
    df_sales_dedup = df_sales.dropDuplicates(['sale_id', 'user_id', 'timestamp'])
    df_sales_dedup.createOrReplaceTempView("temp_sales")
    df_inventory_dedup = df_inventory.dropDuplicates(['inventory_id', 'timestamp'])
    df_inventory_dedup.createOrReplaceTempView("temp_inventory")
    df_users_dedup = df_users.dropDuplicates(['user_id', 'user_name'])
    df_users_dedup.createOrReplaceTempView("temp_users")
    
    # Merge sales
    spark.sql("""
        MERGE INTO glue.icebergdb.sales AS target
        USING temp_sales AS source
        ON target.sale_id = source.sale_id AND target.user_id = source.user_id AND target.timestamp = source.timestamp
        WHEN MATCHED THEN
            UPDATE SET 
            target.price = source.price,
            target.product = source.product,
            target.quantity = source.quantity
        WHEN NOT MATCHED THEN
            INSERT (sale_id, user_id, price, product, quantity, timestamp)
            VALUES (source.sale_id, source.user_id, source.price, source.product, source.quantity, source.timestamp)
    """)

    # Merge inventory
    spark.sql("""
        MERGE INTO glue.icebergdb.inventory AS target
        USING temp_inventory AS source
        ON target.inventory_id = source.inventory_id AND target.timestamp = source.timestamp
        WHEN MATCHED THEN
            UPDATE SET 
            target.product = source.product,
            target.stock_count = source.stock_count
        WHEN NOT MATCHED THEN
            INSERT (inventory_id, product, stock_count, timestamp)
            VALUES (source.inventory_id, source.product, source.stock_count, source.timestamp)
    """)

    # Merge users
    spark.sql("""
        MERGE INTO glue.icebergdb.users AS target
        USING temp_users AS source
        ON target.user_id = source.user_id AND target.user_name = source.user_name
        WHEN MATCHED THEN
            UPDATE SET 
            target.email = source.email,
            target.age = source.age,
            target.gender = source.gender,
            target.registered_on = source.registered_on
        WHEN NOT MATCHED THEN
            INSERT (user_id, user_name, email, age, gender, registered_on)
            VALUES (source.user_id, source.user_name, source.email, source.age, source.gender, source.registered_on)
    """)

    # Aggregate Sales
    df_sales_agg = df_sales.groupBy('product').agg(
        F.sum('quantity').alias('total_quantity_sold'),
        F.sum(F.col('price') * F.col('quantity')).alias('total_revenue')
    )

    # Aggregate Inventory
    df_inventory_agg = df_inventory.groupBy("product").agg(
        F.sum("stock_count").alias("inventory")
    ).withColumn("restock_needed", F.col("inventory") < 0)

    # Update Analaytics Table (Overwrite with versioning)
    df_sales_agg.write.format("iceberg").mode("overwrite").save("glue.icebergdb.sales_aggregated")
    df_inventory_agg.write.format("iceberg").mode("overwrite").save("glue.icebergdb.inventory_aggregated")