from airflow.decorators import dag, task
import boto3, time
from datetime import datetime, timedelta
from pipelines.extract_reddit import extract_reddit
from pipelines.aws_s3_pipeline import upload_to_s3_pipeline
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.hooks.glue_crawler import GlueCrawlerHook
from dags.utils.constants import *

GLUE_JOB_NAME = 'reddit_glue_job_2025'
GLUE_CRAWLER_NAME = 'reddit_crawler_2025'

DEFAULT_ARGS = {
    "owner": "Melvin Sarabia",
    "start_date": datetime(2025, 10, 18),
}

@dag(
    dag_id="reddit_api_data_pipeline",
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    catchup=False,
    tags=["Reddit", "Pipeline"],
)
def reddit_pipeline():

    @task(retries=3, retry_delay=timedelta(seconds=15))
    def extract_task():
        """Extract data from Reddit."""
        file_postfix = datetime.now().strftime("%Y%m%d")
        file_name = f"reddit_{file_postfix}"
        return extract_reddit(
            file_name=file_name,
            subreddit="learnprogramming",
            time_filter="day",
            limit=100,
        )

    @task(retries=3, retry_delay=timedelta(seconds=15))
    def upload_task(file_path: str):
        """Upload extracted data to S3."""
        upload_to_s3_pipeline(file_path)
        return file_path
        
    @task(retries=3, retry_delay=timedelta(seconds=15))
    def run_glue_job(file_name: str):

        glue = boto3.client(
            "glue",
            region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_ACCESS_KEY,
        )
        
        input_path = f"s3://{AWS_BUCKET_NAME}/raw/{file_name.split('/')[-1]}"
        output_path = f"s3://{AWS_BUCKET_NAME}/analytics/"
        
        print(f"Starting Glue job: {GLUE_JOB_NAME}")
        response = glue.start_job_run(
            JobName=GLUE_JOB_NAME,
            Arguments={
                "--JOB_NAME": GLUE_JOB_NAME,
                "--INPUT_PATH": input_path,
                "--OUTPUT_PATH": output_path
            }
        )        
        
        job_run_id = response["JobRunId"]

        # Get the Job Run completion status
        while True:
            status_response = glue.get_job_run(JobName=GLUE_JOB_NAME, RunId=job_run_id)
            state = status_response["JobRun"]["JobRunState"]
            job_run = status_response["JobRun"]
      
            print(f"Glue job {job_run_id} status: {state}")
            if state in ["SUCCEEDED", "FAILED", "STOPPED"]:
                break

            time.sleep(45)  

        # Get the logs if not succeeded
        if state != "SUCCEEDED":
            error_message = job_run.get("ErrorMessage", "No error message returned by Glue.")
            print(f"Glue job failed with state: {state}")
            print(f"   Error message: {error_message}")
            raise Exception(f"Glue job failed with state: {state}. Error: {error_message}")

        print(f"Glue job {job_run_id} completed successfully.")
        return job_run_id
    
    @task(retries=3, retry_delay=timedelta(seconds=15))
    def run_glue_crawler():
        from botocore.exceptions import ClientError
        
        session = boto3.Session(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_ACCESS_KEY,
            region_name=AWS_REGION
        )
        glue_client = session.client("glue")

        try:
            response = glue_client.start_crawler(Name=GLUE_CRAWLER_NAME)
            print(f"Crawler '{GLUE_CRAWLER_NAME}' started successfully.")
            return response
        except ClientError as e:
            if e.response['Error']['Code'] == 'CrawlerRunningException':
                print(f"Crawler '{GLUE_CRAWLER_NAME}' is already running.")
            else:
                print(f"Failed to start crawler: {e}")
            raise

    extracted_file = extract_task()
    uploaded = upload_task(extracted_file)
    glue_run = run_glue_job(uploaded)
    crawler = run_glue_crawler()
    
    extracted_file >> uploaded >> glue_run >> crawler
    
reddit_pipeline()