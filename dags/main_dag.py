from airflow.sdk.dag import dag
from airflow.sdk.task import task
from datetime import datetime
from pipelines.extract_reddit import extract_reddit
from pipelines.aws_s3_pipeline import upload_to_s3_pipeline
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.hooks.glue_crawler import GlueCrawlerHook
from utils.contants import *
from dotenv import load_dotenv

load_dotenv() 

GLUE_JOB_NAME = os.getenv("GLUE_JOB_NAME")
AWS_REGION = os.getenv("AWS_REGION")

# 💡 Move dynamic runtime values inside @task — not at DAG parse time
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

    @task
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

    @task
    def upload_task(file_path: str):
        """Upload extracted data to S3."""
        upload_to_s3_pipeline(file_path)
        
    def run_glue_job():
        # Return the operator itself; DO NOT call .execute()
        return GlueJobOperator(
            task_id="run_glue_job_task",
            job_name=GLUE_JOB_NAME,
            region_name=AWS_REGION,
            script_location="s3://aws-glue-assets-381492168293-us-east-1/scripts/reddit_glue_job_2025.py",
            iam_role_name="reddit_pipeline_2025_role",
            wait_for_completion=True,
            verbose=True,
        )
    
    @task()
    def run_glue_crawler():
        """Run Glue crawler after job completes"""
        crawler_hook = GlueCrawlerHook(region_name=AWS_REGION)
        crawler_hook.start_crawler(GLUE_CRAWLER_NAME)
        return "Crawler started"

    extracted_file = extract_task()
    uploaded = upload_task(extracted_file)
    glue_run = run_glue_job()
    crawler = run_glue_crawler()
    
    extracted_file >> uploaded >> glue_run >> crawler
    
reddit_pipeline()
