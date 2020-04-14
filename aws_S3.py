import boto3
import urllib3
import os
from urllib3.exceptions import InsecureRequestWarning


urllib3.disable_warnings(InsecureRequestWarning)


AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')


session = boto3.Session(aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
conn = session.resource('s3')
bucket_name = 'twilio-analysis'
prefix_name = 'twilio-billing/'
year_path_name = ''
month_path_name = ''
bucket = conn.Bucket(bucket_name)


def upload_file(file_to_upload, path):
    conn.Object(bucket_name, prefix_name + path).put(Body=file_to_upload.getvalue())
    file_to_upload.close()