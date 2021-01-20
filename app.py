import boto3
import os
from dotenv import load_dotenv

#Function to download the file from S3 Bucket
def download_file_from_s3(bucket_name):
    #Instantiate s3 resource
    s3 = boto3.resource('s3')
    # select bucket
    my_bucket = s3.Bucket(bucket_name)
    #download all files into current directory
    for s3_object in my_bucket.objects.all():
        filename = s3_object.key
        my_bucket.download_file(s3_object.key, filename)

#Function to upload the particular file into the s3 Bucket
def upload_file_into_s3(file_name, bucket_name):
    #Connects to s3 using the Access key Id and secret access key stored in .env file
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
        )

    response = s3_client.upload_file(file_name, bucket_name, file_name)

#loads the .env file
load_dotenv() 
#name of the file
file_name = 'userdata1.parquet'
#name of the bucket
bucket_name = 'your bucket name'

download_file_from_s3(bucket_name)

upload_file_into_s3(file_name, bucket_name)
