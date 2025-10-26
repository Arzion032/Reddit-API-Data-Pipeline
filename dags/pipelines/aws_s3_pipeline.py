from etls.aws_etls import *
from utils.contants import AWS_BUCKET_NAME


def upload_to_s3_pipeline(file_path: str):
    # Uploads csv to S3Bucket
    print(f"Uploading {file_path} to S3...")
    
    s3 = s3_conn()
    create_bucket(s3, AWS_BUCKET_NAME)
    upload_to_s3(
        s3,
        file_path,
        AWS_BUCKET_NAME,
        file_path.split('/')[-1]
    )