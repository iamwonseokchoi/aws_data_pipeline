import gzip
import boto3
from datetime import datetime


class EMRServerless:
    def __init__(self, emr_client, application_id: str = None) -> None:
        self.application_id = application_id
        self.s3_log_prefix = "emr-serverless-logs"
        self.app_type = "SPARK"  
        self.client = emr_client

    def __str__(self):
        return f"EMR Serverless {self.app_type} Application: {self.application_id}"

    def create_application(self, name: str, release_label: str, wait: bool = True):
        """
        Create a new application with the provided name and release_label - the application needs to be started after.
        """
        if self.application_id is None:
            response = self.client.create_application(
                name=name, releaseLabel=release_label, type=self.app_type
            )
            self.application_id = response.get("applicationId")
        else:
            self.start_application()

        app_ready = False
        while wait and not app_ready:
            response = self.client.get_application(applicationId=self.application_id)
            app_ready = response.get("application").get("state") in ['CREATED', 'STARTED']

    def start_application(self, wait: bool = True) -> None:
        """
        Start the application - by default, wait until the application is started.
        """
        if self.application_id is None:
            raise Exception(
                "No application_id - please use creation_application first."
            )

        self.client.start_application(applicationId=self.application_id)

        app_started = False
        while wait and not app_started:
            response = self.client.get_application(applicationId=self.application_id)
            app_started = response.get("application").get("state") == "STARTED"

    def stop_application(self, wait: bool = True) -> None:
        """
        Stop the application - by default, wait until the application is stopped.
        """
        self.client.stop_application(applicationId=self.application_id)

        app_stopped = False
        while wait and not app_stopped:
            response = self.client.get_application(applicationId=self.application_id)
            app_stopped = response.get("application").get("state") == "STOPPED"

    def delete_application(self) -> None:
        """
        Delete the application - it must be stopped first.
        """
        self.client.delete_application(applicationId=self.application_id)

    def cancel_spark_job(self, job_id: str):
        return self.client.cancel_job_run(applicationId=self.application_id, jobRunId=job_id)

    def run_spark_job(
            self,
            name: str,
            script_location: str,
            venv_name: str,
            venv_location: str,
            modules_location: str,
            job_role_arn: str,
            arguments: list,
            s3_bucket_name: str,
            wait: bool = True,

    ) -> str:
        """
        Runs the Spark job identified by `script_location`. Arguments can also be provided via the `arguments` parameter.
        By default, spark-submit parameters are hard-coded and logs are sent to the provided s3_bucket_name.
        This method is blocking by default until the job is complete.
        """
        driver = f'spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./{venv_name}/bin/python'
        pyspark = f'spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./{venv_name}/bin/python'
        executor = f'spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python'

        response = self.client.start_job_run(
            applicationId=self.application_id,
            executionRoleArn=job_role_arn,
            name=f'{name}-{datetime.today().strftime("%Y-%m-%d %H:%M:%S")}',
            jobDriver={
                "sparkSubmit": {
                    "entryPoint": script_location,
                    "entryPointArguments": arguments,
                    "sparkSubmitParameters": f"--conf spark.executor.cores=2 "
                                                f"--conf spark.executor.memory=4g "
                                                f"--conf spark.driver.cores=2 "
                                                f"--conf spark.driver.memory=4g "
                                                f"--conf spark.executor.instances=2 "
                                                f"--conf spark.submit.pyFiles={modules_location} "
                                                f"--conf spark.archives={venv_location}#environment "
                                                f"--conf {driver} "
                                                f"--conf {pyspark} "
                                                f"--conf {executor}"
                }
            },
            configurationOverrides={
                'applicationConfiguration': [],
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {
                        "logUri": f"s3://{s3_bucket_name}/{self.s3_log_prefix}"
                    }
                },

            },
        )
        job_run_id = response.get("jobRunId")

        job_done = False
        while wait and not job_done:
            jr_response = self.get_job_run(job_run_id)
            job_done = jr_response.get("state") in [
                "SUCCESS",
                "FAILED",
                "CANCELLING",
                "CANCELLED",
            ]

        return job_run_id

    def get_job_run(self, job_run_id: str) -> dict:
        response = self.client.get_job_run(
            applicationId=self.application_id, jobRunId=job_run_id
        )
        return response.get("jobRun")

    def fetch_driver_log(
            self, s3_bucket_name: str, job_run_id: str, log_type: str = "stdout"
    ) -> str:
        """
        Access the specified `log_type` Driver log on S3 and return the full log string.
        """
        s3_client = boto3.client("s3")
        file_location = f"{self.s3_log_prefix}/applications/{self.application_id}/jobs/{job_run_id}/SPARK_DRIVER/{log_type}.gz"
        try:
            response = s3_client.get_object(Bucket=s3_bucket_name, Key=file_location)
            file_content = gzip.decompress(response["Body"].read()).decode("utf-8")
        except s3_client.exceptions.NoSuchKey:
            file_content = ""
        return str(file_content)