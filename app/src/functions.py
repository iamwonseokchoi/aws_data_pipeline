import os
import boto3
import awswrangler as wrangler
import spacy
from elasticsearch import Elasticsearch
from emr import EMRServerless

from dotenv import load_dotenv

# Load environment variables
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

url = os.environ.get("URL")
elastic_api_key = os.environ.get("ELASTIC_API_KEY")
es = Elasticsearch(url, api_key=elastic_api_key)
region_name = "ap-northeast-2"
aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
nlp = spacy.load("en_core_web_sm")
session = boto3.Session(region_name="ap-northeast-2", aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
client = session.client("emr-serverless", region_name="ap-northeast-2")
APPLICATION_ID = os.environ.get("APPLICATION_ID")
APPLICATION_NAME = os.environ.get("APPLICATION_NAME")
S3_LOGS_BUCKET = os.environ.get("S3_LOGS_BUCKET")
EMR_JOB_ROLE_ARN = os.environ.get("EMR_JOB_ROLE_ARN")
S3_BUCKET = os.environ.get("S3_BUCKET")
DEFAULT_INPUT_PATH = f's3://{S3_BUCKET}/data'
ENVIRONMENT_PATH = f's3://{S3_BUCKET}/emr/environment'
ENVIRONMENT_FILE = 'environment.tar.gz'
FULL_ENVIRONMENT_PATH = f'{ENVIRONMENT_PATH}/{ENVIRONMENT_FILE}'
MODULE_PATH = f's3://{S3_BUCKET}/emr/modules'
MODULE_FILE = 'src.zip'
FULL_MODULE_PATH = f'{MODULE_PATH}/{MODULE_FILE}'
SCRIPT_PATH = f's3://{S3_BUCKET}/emr/scripts'
SCRIPT_FILE = 'spark_job.py'
FULL_SCRIPT_PATH = f'{SCRIPT_PATH}/{SCRIPT_FILE}'
emr_serverless = EMRServerless(emr_client=client, application_id=APPLICATION_ID)


def construct_query():
    query = {
        "size": 10000,
        "_source": ["message"],
        "query": {
            "match_all": {}
        }
    }
    return query

def execute_query(query):
    res = es.search(index="es_events-*", body=query)
    messages = [hit["_source"]["message"] for hit in res["hits"]["hits"]]
    return messages

def athena_query(query: str):
    session = boto3.Session(
    region_name=region_name,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
    )
    df = wrangler.athena.read_sql_query(
        sql=query,
        database="icebergdb",  
        boto3_session=session  
    )
    return df

def preprocess(text):
    doc = nlp(text)
    return " ".join([token.lemma_ for token in doc])

def search(query):
    processed_query = preprocess(query)
    body = {
        "query": {
            "multi_match": {
                "query": processed_query,
                "fields": ["message", "processed_message"],
                "fuzziness": "AUTO"
            }
        }
    }
    response = es.search(index="es_events-*", body=body)
    return response['hits']

def start_emr_job():
    job_run_id = emr_serverless.run_spark_job(
        name=APPLICATION_NAME,
        script_location=f"{FULL_SCRIPT_PATH}",
        venv_name="environment",
        venv_location=f'{FULL_ENVIRONMENT_PATH}',
        modules_location=f'{FULL_MODULE_PATH}',
        job_role_arn=EMR_JOB_ROLE_ARN,
        arguments=["PROD"],
        s3_bucket_name=S3_LOGS_BUCKET,
        wait=False
    )
    return job_run_id

def get_emr_job_status(job_id):
    return emr_serverless.get_job_run(job_id).get("state")

def monitor_emr_job(job_id):
    job_state = get_emr_job_status(job_id)
    if job_state in ["SUCCESS", "FAILED", "CANCELLING", "CANCELLED"]:
        return job_state
    else:
        return "RUNNING"