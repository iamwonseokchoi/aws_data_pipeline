name := "MSKSparkStreaming"

version := "1.0"

scalaVersion := "2.12.17"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "3.4.1",
    "org.apache.spark" %% "spark-sql" % "3.4.1",
    "org.apache.spark" %% "spark-streaming" % "3.4.1",
    "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.4.1",
    "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.4.1",
    "org.apache.hadoop" % "hadoop-aws" % "3.3.2", 
    "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.566"
)
