import time
import requests
import boto3, os
from dotenv import load_dotenv

load_dotenv()

# requests.get('http://localhost:5000/ping')

print(os.getenv('AWS_ACCESS_KEY_ID'))
db = boto3.client('dynamodb',
                  region_name='us-east-1',
                  aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                  aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                  aws_session_token=os.getenv('AWS_SESSION_TOKEN'))

# db = getClient('dynamodb')
db_resp = db.create_table(
    TableName='temp',
    KeySchema=[
        {
            'AttributeName': 'username',
            'KeyType': 'HASH'
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'username',
            'AttributeType': 'S'
        },
    ],
    BillingMode='PAY_PER_REQUEST'
)

print(db_resp)
# print({'Questions_get': response})