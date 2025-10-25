from etls.aws_etls import *
from utils.contants import AWS_BUCKET_NAME

def upload_to_s3_pipeline(ti):
    file_path= ti.xcom_pull(task_ids='reddit_extraction', key='return_value')
    print(file_path)
    s3 = s3_conn()
    create_bucket(s3, AWS_BUCKET_NAME)
    upload_to_s3(s3, file_path, AWS_BUCKET_NAME, file_path.split('/')[-1])