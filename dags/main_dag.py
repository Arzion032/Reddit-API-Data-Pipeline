from airflow.decorators import dag, task
from datetime import datetime
from pipelines.extract_reddit import extract_reddit
from pipelines.aws_s3_pipeline import upload_to_s3_pipeline

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

    extracted_file = extract_task()
    upload_task(extracted_file)

reddit_pipeline()
