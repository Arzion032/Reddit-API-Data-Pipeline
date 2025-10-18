from airflow import DAG
from datetime import datetime
import os, sys 
from airflow.providers.standard.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.extract_reddit import extract_reddit

file_postfix = datetime.now().strftime("%Y%m%d")

default_args = {
    'owner': 'Melvin Sarabia',
    'start_date': datetime(2025, 10, 18)
}

with DAG(
    dag_id='reddit_api_data_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=['Reddit', 'Pipeline']
) as dag:
    
    # Extract data from Reddit
    extract = PythonOperator(
        task_id='reddit_extraction',
        python_callable=extract_reddit,
        op_kwargs={
            'file_name': f'reddit_{file_postfix}',
            'subreddit': 'dataengineering',
            'time_filter': 'day',
            'limit': 100,
        }
    )
