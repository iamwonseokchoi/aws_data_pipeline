import boto3

# Constants
from utils.consts import *
from processing.emr import EMRServerless

session = boto3.Session(region_name=AWS_REGION,aws_access_key_id=AWS_ACCESS_KEY,aws_secret_access_key=AWS_SECRET_KEY)
client = session.client("emr-serverless", region_name=AWS_REGION)

# Fixed to existing EMR SErverless App ID
APPLICATION_ID = None
APPLICATION_NAME = 'BatchOnDemand'

# Start EMRServerless Spark Application
def main():
    emr_serverless = EMRServerless(emr_client=client, application_id=APPLICATION_ID)
    emr_serverless.start_application()

    job_dict = {}
    job_run_id = None

    try:
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
        print(f"Submitting new Spark job with id {job_run_id}")

    except Exception as e:
        print(f'Error while submitting job: \n{e}')

        for job_run_id in job_dict.keys():
            job_status = emr_serverless.cancel_spark_job(job_id=job_run_id)
            print(f'Job {job_run_id} cancelled')

        raise e

    job_dict[job_run_id] = False

    return


if __name__ == "__main__":
    main()