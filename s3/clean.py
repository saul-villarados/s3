import boto3
from dotenv import load_dotenv
import os

load_dotenv()

s3 = boto3.client('s3', aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"), 
                  aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"), 
                  region_name=os.getenv("AWS_REGION"))
bucket = os.getenv("S3_BUCKET_NAME")

# Listar y eliminar objetos en 'images/'
objects = s3.list_objects_v2(Bucket=bucket, Prefix='images/')['Contents']
delete_keys = [{'Key': obj['Key']} for obj in objects]
if delete_keys:
    s3.delete_objects(Bucket=bucket, Delete={'Objects': delete_keys})
print("Eliminado todo en images/")