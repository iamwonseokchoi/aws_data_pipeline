# %%
import os

AWS_ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.environ.get('AWS_SECRET_KEY')

AWS_PROFILE = 'emr-serverless'
AWS_REGION = 'ap-northeast-2'
S3_BUCKET = 'amore-bucket'
S3_LOGS_BUCKET = 'amore-bucket'
EMR_JOB_ROLE_ARN = os.environ.get('EMR_JOB_ROLE_ARN')

DEFAULT_INPUT_PATH = f's3://{S3_BUCKET}/data'

UPDATE_ENVIRONMENT = False  # Only on init
UPDATE_MODULES = False  # Only if code change

ENVIRONMENT_PATH = f's3://{S3_BUCKET}/emr/environment'
ENVIRONMENT_FILE = 'environment.tar.gz'
FULL_ENVIRONMENT_PATH = f'{ENVIRONMENT_PATH}/{ENVIRONMENT_FILE}'

MODULE_PATH = f's3://{S3_BUCKET}/emr/modules'
MODULE_FILE = 'src.zip'
FULL_MODULE_PATH = f'{MODULE_PATH}/{MODULE_FILE}'

SCRIPT_PATH = f's3://{S3_BUCKET}/emr/scripts'
SCRIPT_FILE = 'spark_job.py'
FULL_SCRIPT_PATH = f'{SCRIPT_PATH}/{SCRIPT_FILE}'

APPLICATION_ID = None