import s3fs
from utils.contants import AWS_ACCESS_KEY_ID, AWS_ACCESS_KEY

def connect_to_s3():
    try:
        s3 = s3fs.S3FileSystem(anon=False,
                              key=AWS_ACCESS_KEY_ID,
                              secret=AWS_ACCESS_KEY)
        return s3
    except  Exception as e:
        print(e)
        
def create_bucket_if_not_exist(s3: s3fs.S3FileSystem, bucket:str):
    try:
        if not s3.exists(bucket):
            s3.mkdir(bucket)
            print('Bucket created!')
        else: 
            print('Bucker already existed!')
    
    except Exception as e:
        print(f'Error: {e}')