from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
import subprocess
import zipfile
import os
import boto3

default_args = {
    'owner': 'you',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def download_from_s3():
    s3_client = boto3.client('s3')
    s3_client.download_file('amore-bucket', 'emr/modules/src.zip', '/home/ubuntu/emr/src.zip')

def unzip_and_run_file():
    try:
        with zipfile.ZipFile('/home/ubuntu/emr/src.zip', 'r') as zip_ref:
            zip_ref.extractall('/home/ubuntu/emr/')
        subprocess.run(["python", "/home/ubuntu/emr/src/airflow_run.py"])
    except Exception as e:
        raise ValueError(f"Failed to execute unzip_and_run_file: {e}")

with DAG(
    's3_to_local_and_run_script',
    default_args=default_args,
    description='Dynamically acquire source from s3 and run',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(0),
    tags=['emr'],
) as dag:

    start = DummyOperator(
        task_id='start',
    )

    end = DummyOperator(
        task_id='end',
    )

    download_from_s3 = PythonOperator(
        task_id='download_from_s3',
        python_callable=download_from_s3,
    )

    unzip_and_run = PythonOperator(
        task_id='unzip_and_run',
        python_callable=unzip_and_run_file,
    )

    start >> download_from_s3 >> unzip_and_run >> end
