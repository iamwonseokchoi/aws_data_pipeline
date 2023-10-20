# %%
import pyspark
from pyspark.sql import SparkSession
from ..utils.consts import AWS_ACCESS_KEY, AWS_SECRET_KEY


def get_spark_session(env: str = None, app_name: str = 'SparkApp') -> SparkSession:
    conf = (
        pyspark.SparkConf()
            .setAppName(app_name)
            .set('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.0,software.amazon.awssdk:bundle:2.17.178,software.amazon.awssdk:url-connection-client:2.17.178')
            .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
            .set('spark.sql.catalog.glue', 'org.apache.iceberg.spark.SparkCatalog')
            .set('spark.sql.catalog.glue.catalog-impl', 'org.apache.iceberg.aws.glue.GlueCatalog')
            .set('spark.sql.catalog.glue.warehouse', 's3a://amore-bucket/warehouse')
            .set('spark.sql.catalog.glue.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
            .set('spark.hadoop.fs.s3a.access.key', AWS_ACCESS_KEY)
            .set('spark.hadoop.fs.s3a.secret.key', AWS_SECRET_KEY)
    )

    builder = SparkSession.builder.config(conf=conf)

    if env is not None and env == 'DEV':
        spark = builder.master('local[*]').getOrCreate()
    else:
        spark = builder.getOrCreate()

    return spark


def run_iceberg_ddl(run: str = False, upstream_spark: SparkSession = None) -> None:
    if run:
        spark = upstream_spark
        # sales table
        spark.sql("""
            CREATE TABLE IF NOT EXISTS glue.icebergdb.sales
            (sale_id string, user_id string, price float, product string, quantity BIGINT, timestamp string)
            USING iceberg          
        """)

        # inventory table
        spark.sql("""
            CREATE TABLE IF NOT EXISTS glue.icebergdb.inventory
            (inventory_id string, product string, stock_count BIGINT, timestamp string)
            USING iceberg
        """)

        # users table
        spark.sql("""
            CREATE TABLE IF NOT EXISTS glue.icebergdb.users
            (user_id string, user_name string, email string, age BIGINT, gender string, registered_on string)
            USING iceberg
        """)

        # aggregated sales table
        spark.sql("""
            CREATE TABLE IF NOT EXISTS glue.icebergdb.sales_aggregated
            (product string, total_quantity_sold BIGINT, total_revenue float)
            USING iceberg
        """)

        # aggregated inventory table
        spark.sql("""
            CREATE TABLE IF NOT EXISTS glue.icebergdb.inventory_aggregated
            (product string, inventory BIGINT, restock_needed boolean)
            USING iceberg
        """)
    return 