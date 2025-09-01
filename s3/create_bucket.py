import boto3
import os
from dotenv import load_dotenv
from utils.logger import get_logger
from utils.custom_exception import CustomException
load_dotenv()

logger = get_logger(__name__)


def main():
    try:
        logger.info('setting up aws credentials')
        s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION')
        )

        bucket_name = os.getenv('S3_BUCKET_NAME')  
        logger.info('Succesully logged in')

    except Exception as e:
        logger.error(f'Failed to log in with credentials due to: {str(e)}')
        raise CustomException('Error configuring credentials', e)

    try:
        s3_client.create_bucket(Bucket=bucket_name)
        logger.info(f"Bucket '{bucket_name}' successfully created in us-east-1.")
    except Exception as e:
        logger.error(f'Failed to create Bucket due to : {str(e)}')
        raise CustomException 
    
if __name__ == '__main__':
    main()