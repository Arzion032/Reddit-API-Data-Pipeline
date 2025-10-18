from airflow import DAG
from datetime import datetime
import os, sys 
from airflow.operators.python import PythonOperator
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

default_args = {
    'owner': 'Melvin Sarabia',
    'start_date': datetime(2025, 10, 18)
}

file_postfix = datetime.now().strftime("%Y%m%d")

dag = DAG(
    dag_id='reddit_api_data_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    cathcup=False,
    tags=['Reddit' 'Pipeline']
)

# Extract data from Reddit
extract=PythonOperator(
    task_id='reddit_extraction',
    python_callable=extract_reddit,
    op_kwargs={
        'file_name': f'reddit_{file_postfix}',
        'subreddit': 'data_engineering',
        'time_filter': 'day',
        'limit': 100,
    },
    dag=dag
)
