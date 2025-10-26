import s3fs
from utils.contants import AWS_ACCESS_KEY_ID, AWS_ACCESS_KEY

def s3_conn():
    try:
        s3 = s3fs.S3FileSystem(anon=False,
                              key=AWS_ACCESS_KEY_ID,
                              secret=AWS_ACCESS_KEY)
        return s3
    except  Exception as e:
        print(e)
        
def create_bucket(s3: s3fs.S3FileSystem, bucket:str):
    try:
        if not s3.exists(bucket):
            s3.mkdir(bucket)
            print('Bucket created!')
        else: 
            print('Bucker already existed!')
    
    except Exception as e:
        print(f'Error: {e}')
        
def upload_to_s3(s3: s3fs.S3FileSystem, file_path:str, bucket:str, s3_file_name: str):
    try:
        s3.put(file_path, bucket+'/raw/'+ s3_file_name)
        print('File uploaded to s3')
    except FileNotFoundError:
        print('File not found')
        
    