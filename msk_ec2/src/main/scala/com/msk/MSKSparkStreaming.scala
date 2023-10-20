import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object MSKSparkStreaming {
    def main(args: Array[String]): Unit = {
    
    val sparkConf = new SparkConf().setAppName("MSKSparkStreaming")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

    import spark.implicits._

    // JSON schema
    val jsonSchema = StructType(Seq(
        StructField("table", StringType, nullable = true),
        StructField("data", StringType, nullable = true)
    ))

    // Define schema for each table
    val userSchema = StructType(Array(
        StructField("user_id", StringType, nullable = false),
        StructField("user_name", StringType, nullable = true),
        StructField("email", StringType, nullable = true),
        StructField("age", IntegerType, nullable = true),
        StructField("gender", StringType, nullable = true),
        StructField("registered_on", StringType, nullable = false)
    ))

    val salesSchema = StructType(Array(
        StructField("sale_id", StringType, nullable = false),
        StructField("product", StringType, nullable = true),
        StructField("user_id", StringType, nullable = true),
        StructField("price", DoubleType, nullable = true),
        StructField("quantity", IntegerType, nullable = true),
        StructField("timestamp", StringType, nullable = false)
    ))

    val inventorySchema = StructType(Array(
        StructField("inventory_id", StringType, nullable = false),
        StructField("product", StringType, nullable = true),
        StructField("stock_count", IntegerType, nullable = true),
        StructField("timestamp", StringType, nullable = false)
    ))

    // MSK parameters
    val kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "")
    val kafkaParams = Map(
        "kafka.bootstrap.servers" -> kafkaBootstrapServers,
        "subscribe" -> "iceberg_events",
        "group.id" -> "spark"
    )

    // MSK Read
    val kafkaStreamDF = spark.readStream
        .format("kafka")
        .options(kafkaParams)
        .load()

    // Deserialize JSON, getting only the table name for now
    val deserializedDF = kafkaStreamDF.selectExpr("CAST(value AS STRING) as json")
        .select(from_json($"json", jsonSchema).as("data"))
        .select("data.*")

    val query = deserializedDF.writeStream
        .option("checkpointLocation", "s3a://amore-bucket/checkpoints/")
        .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
            if (batchDF.count() > 0) {
                val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHH"))

                // Cache Dataframe (Simulate for high performance needs)
                batchDF.cache()

                val userDF = batchDF.filter($"table" === "user")
                    .select(from_json($"data", userSchema).as("data")).select("data.*")

                val salesDF = batchDF.filter($"table" === "sales")
                    .select(from_json($"data", salesSchema).as("data")).select("data.*")

                val inventoryDF = batchDF.filter($"table" === "inventory")
                    .select(from_json($"data", inventorySchema).as("data")).select("data.*")

                // Determine the number of partitions dynamically based on load
                val userPartitions = if (userDF.count() > 1000) 10 else 5
                val salesPartitions = if (salesDF.count() > 1000) 10 else 5
                val inventoryPartitions = if (inventoryDF.count() > 1000) 10 else 5

                // Write to S3 with dynamic partitioning and parallelization
                if (userDF.count() > 0) {
                    userDF.repartition(userPartitions).write
                        .mode("append")
                        .json(s"s3a://amore-bucket/raw/user/user-$timestamp.json")
                }

                if (salesDF.count() > 0) {
                    salesDF.repartition(salesPartitions).write
                        .mode("append")
                        .json(s"s3a://amore-bucket/raw/sales/sales-$timestamp.json")
                }

                if (inventoryDF.count() > 0) {
                    inventoryDF.repartition(inventoryPartitions).write
                        .mode("append")
                        .json(s"s3a://amore-bucket/raw/inventory/inventory-$timestamp.json")
                }

                batchDF.unpersist()
            }
            ()  // return Unit
        }
        .start()

        query.awaitTermination()
    }
}