# End to End Data Engineering Pipeline using AWS Services

## Overview
End to end project utilizing AWS services for a cloud native and containerized setup. \
Every component of the project is dockerized or integrated with AWS infrastructure, and was designed to operate on a single VPC network. 

The project generates mock data designed to produce to a MSK (Kafka) server whilst also producing output logs. Aims to simulate an e-commerce company's customer-facing web application at a scale manageable for personal AWS accounts. Generates around 100MBs of data to push through the pipeline every 4-5 hours but is cloud native scalable (Tested up to 10GB of output per hour as-is but drove up the AWS bill too high). 

Mock data is processed via Spark structured stream hosted on AWS MSK with logs pushed through to Elastic Stack on EC2s with backups dumped to S3. Initial raw data from streaming is batch processed via EMR serverless jobs fully orchestrated by Airflow (hosted on EC2). Further down the data pipeline, data is processed to mock a data warehouse where Apache Iceberg is converged with Glue Data Catalog serving as the metastore. Warehouse simulated data is then linked via Athena and Pyiceberg to allow for analysis on the go. Script is provided to analyze data directly on Athena console, as well as local environments via AWS wrangler and boto3. S3 buckes served as raw storage, log storage (for Logstash, EMR, Spark Stream), and warehouse.

As a simple showcase, a sample mock streamlit frontend was created to bring together warehouse and Elasticsearch NoSQL data to simulate data consuming. To simulate a touch on both the data analytics and data science aspects of the pipeline, very light components on search analytics and NLP (using readily available SpaCy model) was provisioned. 

## Technologies Used:
- **AWS:** IAM, Secrets Manager, KMS, Glue, Athena, MSK, S3, EMR, EC2, VPC
- **Languages:** Python, Scala
- **Tools:** Apache Airflow, Elastic Stack (Logstash, Elasticsearch, Kibana), Docker, Docker-compose, Spark SQL, Spark Structured Streaming, Apache Iceberg, Streamlit
- **Others (lightly used):** TD-IDF(BM25), SpaCy NLP(en_core_web_sm 3.7.0), Delta Lake, Lambda, Code Pipeline, Cloud Watch

## Modules/Directories
- app: Streamlit mock frontend
- data: Initial data producer script (Uses Faker)
- ec2_airflow: Airflow provisioning Dockerized (Airflow is very baremetal in this project, configured simply just for REST API client use and simple orchestration of EMR Serverless)
- ec2_elastic: Elastic Stack (logstash.conf configuration and other small modifications were done to the existing Docker-compose to fit the use-case. Source repository from where the Elastic Docker-compose was pulled from is cited in the Docker-compose file)
- emr_glue: EMR Serverles and Glue configs for Iceberg with Glue Data Catalog as metastore
- msk_ec2: Spark structured stream scala job (Also Dockerized, as running stream jobs on EMR clusters turned out to be ultra expensive)

## To Run
Each module can technically be standalone and can be booted up using the Dockerfiles.
For Spark components, the spark-submit commands can be copied from the Docker CMDs for local testing. 

## Architecture
![Architecture](app/images/archi.png)

## Frontend App (Streamlit)
Currently a version serving on Streamlit Cloud (Requires login): https://awsdatapipeline.streamlit.app/

![App](app/images/app.png)

--
**E.O.D**