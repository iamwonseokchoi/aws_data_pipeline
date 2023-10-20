FROM apache/spark:3.4.1-scala2.12-java11-r-ubuntu

WORKDIR /app

USER root
RUN mkdir -p /opt/spark/.ivy2/cache && chmod 777 /opt/spark/.ivy2/cache
RUN wget -P /opt/spark/jars/ https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar
RUN wget -P /opt/spark/jars/ https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.566/aws-java-sdk-bundle-1.12.566.jar

COPY target/scala-2.12/msksparkstreaming_2.12-1.0.jar /app

ENV SPARK_CLASSPATH /opt/spark/jars/

CMD ["/opt/spark/bin/spark-submit", \
        "--class", "MSKSparkStreaming", \
        "--master", "local[*]", \
        "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1", \
        "/app/msksparkstreaming_2.12-1.0.jar"]
