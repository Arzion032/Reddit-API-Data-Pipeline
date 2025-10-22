def upload_to_s3_pipeline(ti):
    file_name= ti.xcom_pull(task_ids='reddit_extraction', key='return_value')